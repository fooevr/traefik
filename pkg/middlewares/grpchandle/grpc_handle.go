package grpchandle

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	_ "encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/containous/traefik/v2/pkg/cache"
	_ "github.com/containous/traefik/v2/pkg/cache"
	vfasys "github.com/containous/traefik/v2/pkg/cache/proto"
	"github.com/containous/traefik/v2/pkg/config/dynamic"
	"github.com/containous/traefik/v2/pkg/log"
	"github.com/containous/traefik/v2/pkg/middlewares"
	"github.com/containous/traefik/v2/pkg/tracing"
	"github.com/docker/distribution/uuid"
	"github.com/golang/protobuf/proto"
	empty "github.com/golang/protobuf/ptypes/empty"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	dynamicProto "github.com/jhump/protoreflect/dynamic"
	"github.com/mwitkow/grpc-proxy/proxy"
	"github.com/opentracing/opentracing-go/ext"
	gca "github.com/patrickmn/go-cache"
	_ "github.com/vulcand/oxy/buffer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	typeName = "gRPCHandle"
)

type grpcHandle struct {
	next             http.Handler
	name             string
	grpc             *grpc.Server
	websocketWrapper *grpcweb.WrappedGrpcServer
	messages         map[string]*desc.MessageDescriptor

	rpcs map[string]*desc.MethodDescriptor

	incrPrefix      string
	fullPrefix      string
	noPrefix        string
	ttl             int64
	maxVersionCount int64

	currentVersion string
	cacheManager   *cache.CacheManager

	lockers *gca.Cache

	backends      []string
	logger        log.Logger
	protoUpdating int32
	_id           string
}

// New creates a new handler.
func New(ctx context.Context, next http.Handler, config dynamic.GRPCHandler, name string, backends []string) (http.Handler, error) {
	logger := log.FromContext(middlewares.GetLoggerCtx(ctx, name, typeName))
	logger.Info("Creating middleware")

	result := &grpcHandle{
		logger:   logger,
		next:     next,
		name:     name,
		messages: map[string]*desc.MessageDescriptor{},
		rpcs:     map[string]*desc.MethodDescriptor{},
		lockers:  gca.New(time.Hour*10, time.Minute*10),
		_id:      uuid.Generate().String(),
	}
	backendURLs := []string{}
	for _, item := range backends {
		idx := strings.Index(item, "//")
		if idx >= 0 {
			backendURLs = append(backendURLs, item[idx+2:])
		} else {
			backendURLs = append(backendURLs, item)
		}
	}
	result.backends = backendURLs

	director := func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		outCtx, _ := context.WithCancel(ctx)
		mdCopy := md.Copy()
		delete(mdCopy, "user-agent")
		// If this header is present in the request from the web client,
		// the actual connection to the backend will not be established.
		// https://github.com/improbable-eng/grpc-web/issues/568
		delete(mdCopy, "connection")
		outCtx = metadata.NewOutgoingContext(outCtx, mdCopy)
		bk, err := grpc.Dial("10.9.9.131:8080", grpc.WithInsecure(), grpc.WithCodec(proxy.Codec()))
		if err != nil {
			logger.Error(err)
			return nil, nil, err
		}
		return outCtx, bk, nil
	}

	result.grpc = grpc.NewServer(grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
		grpc.CustomCodec(proxy.Codec()),
		grpc.MaxRecvMsgSize(1024*1024*4),
	)
	result.websocketWrapper = grpcweb.WrapServer(result.grpc,
		grpcweb.WithWebsockets(false),
		grpcweb.WithCorsForRegisteredEndpointsOnly(false),
		grpcweb.WithOriginFunc(func(origin string) bool {
			return true
		}),
		grpcweb.WithWebsocketOriginFunc(func(req *http.Request) bool {
			return true
		}),
		grpcweb.WithWebsocketPingInterval(time.Second*5),
		grpcweb.WithAllowedRequestHeaders([]string{"*"}),
		grpcweb.WithAllowNonRootResource(true))

	result.renewProto()
	return result, nil
}

func (this *grpcHandle) renewProto() {
	atomic.StoreInt32(&this.protoUpdating, 1)
	defer func() {
		atomic.StoreInt32(&this.protoUpdating, 0)
	}()
	rand.Seed(time.Now().Unix())
	backend := this.backends[rand.Intn(len(this.backends))]
	conn, err := grpc.Dial(backend, grpc.WithInsecure())
	if err != nil {
		return
	}
	defer conn.Close()
	client := vfasys.NewProtoClient(conn)
	resp, err := client.GetProtoDescription(context.Background(), &empty.Empty{})
	if err != nil {
		return
	}
	if resp.Version == this.currentVersion {
		return
	}
	parser := protoparse.Parser{}
	protos := []*desc.FileDescriptor{}
	fileNames := []string{}
	for name, item := range resp.ProtoFiles {
		dir := filepath.Dir("/tmp/" + name)
		os.MkdirAll(dir, os.ModePerm)
		ioutil.WriteFile("/tmp/"+name, item, os.ModePerm)
		fileNames = append(fileNames, "/tmp/"+name)
	}
	ps, err := parser.ParseFiles(fileNames...)
	if err != nil {
		this.logger.Warn(err)
		return
	}
	protos = append(protos, ps...)

	fileNames = []string{}
	filepath.Walk("./defaultProtos", func(path string, info os.FileInfo, err error) error {
		if info != nil && !info.IsDir() && strings.HasSuffix(info.Name(), ".proto") {
			absPath, _ := filepath.Abs("./" + path)
			fileNames = append(fileNames, absPath)
		}
		return nil
	})

	ps, err = parser.ParseFiles(fileNames...)
	if err != nil {
		this.logger.Warn(err)
		return
	}
	protos = append(protos, ps...)

	for _, file := range protos {
		for _, msgType := range file.GetMessageTypes() {
			this.messages[msgType.GetFullyQualifiedName()] = msgType
		}
		for _, service := range file.GetServices() {
			for _, method := range service.GetMethods() {
				if len(file.GetPackage()) > 0 {
					this.rpcs[fmt.Sprintf("/%s.%s/%s", file.GetPackage(), service.GetName(), method.GetName())] = method
				} else {
					this.rpcs[fmt.Sprintf("/%s/%s", service.GetName(), method.GetName())] = method
				}
			}
		}
	}
	this.currentVersion = resp.Version
	this.ttl = int64(resp.DefaultTTL.Value)
	this.maxVersionCount = int64(resp.DefaultMaxVersion.Value)
	this.fullPrefix = resp.FullCachePrefix.Value
	this.incrPrefix = resp.IncrementCachePrefix.Value
	this.noPrefix = resp.NoCachePrefix.Value
	this.cacheManager = cache.NewCacheManager()
}

func (this *grpcHandle) GetTracingInformation() (string, ext.SpanKindEnum) {
	return this.name, tracing.SpanKindNoneEnum
}

func (this *grpcHandle) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if req.Method == "OPTIONS" {
		this.next.ServeHTTP(rw, req)
		return
	}
	logger := log.FromContext(middlewares.GetLoggerCtx(req.Context(), this.name, typeName))
	if req.Header.Get("Content-Type") == "application/grpc-web-text" {
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		this.websocketWrapper.HandleGrpcWebRequest(rw, req)
		return
	}
	if this.websocketWrapper.IsGrpcWebSocketRequest(req) {
		this.websocketWrapper.HandleGrpcWebsocketRequest(rw, req)
		//this.next.ServeHTTP(rw, req)
		return
	}
	if req.ProtoMajor != 2 || !strings.Contains(req.Header.Get("Content-Type"), "application/grpc") {
		this.next.ServeHTTP(rw, req)
		return
	}
	if atomic.LoadInt32(&this.protoUpdating) == 1 {
		rw.WriteHeader(503)
		this.next.ServeHTTP(rw, req)
		return
	}
	if this.currentVersion == "" {
		this.renewProto()
		rw.WriteHeader(503)
		this.next.ServeHTTP(rw, req)
		return
	}

	spIndex := strings.LastIndex(req.RequestURI, "/")
	rpcName := req.RequestURI[spIndex+1:]
	isNoCache := strings.HasPrefix(rpcName, this.noPrefix)
	if isNoCache {
		this.next.ServeHTTP(rw, req)
		return
	}
	isFullCache := strings.HasPrefix(rpcName, this.fullPrefix)
	isIncrCache := strings.HasPrefix(rpcName, this.incrPrefix)
	if !isFullCache && !isIncrCache {
		this.next.ServeHTTP(rw, req)
		return
	}
	var clientVersion int64
	if req.Header.Get("ts") == "" {
		response(rw, nil, cache.ChangeType_Unchange, 0, nil, 5, errors.New("miss ts header"), this.logger)
		return
	}
	if v, err := strconv.ParseInt(req.Header.Get("ts"), 10, 64); err != nil {
		response(rw, nil, cache.ChangeType_Unchange, 0, nil, 5, errors.New("ts header not int64"), this.logger)
	} else {
		clientVersion = v
	}

	methodDesc := this.rpcs[req.RequestURI]
	if methodDesc == nil {
		response(rw, nil, cache.ChangeType_Unchange, 0, nil, 5, errors.New("request URI "+req.RequestURI+" is not found"), this.logger)
		return
	}
	bts, _ := ioutil.ReadAll(req.Body)
	if len(bts) < 5 {
		response(rw, nil, cache.ChangeType_Unchange, 0, nil, 5, errors.New("parse body to gRPC failed"), this.logger)
		return
	}
	frame := bts[5:]
	cacheIdBuffer := &bytes.Buffer{}
	cacheIdBuffer.Write([]byte(req.RequestURI))
	cacheIdBuffer.Write(frame)
	h := md5.New()
	h.Write(cacheIdBuffer.Bytes())
	cacheId := hex.EncodeToString(h.Sum(nil))
	locker, exists := this.lockers.Get(cacheId)
	if !exists {
		locker = &sync.RWMutex{}
		this.lockers.Add(cacheId, locker, 0)
	}
	// 成功命中则写入并终止处理
	if this.getCache(locker.(*sync.RWMutex), isIncrCache, cacheId, clientVersion, rw) {
		//this.next.ServeHTTP(rw, req)
		return
	}

	code, msg := this.setCache(cacheId, locker.(*sync.RWMutex), bts, req, rw, methodDesc, this, isIncrCache, logger)
	if code == 1000 {
		return
	}
	if code == 503 {
		rw.WriteHeader(503)
		this.next.ServeHTTP(rw, req)
		return
	}
	if code != 0 {
		response(rw, nil, cache.ChangeType_Unchange, 0, nil, code, errors.New(msg), this.logger)
		return
	}
	if !this.getCache(locker.(*sync.RWMutex), isIncrCache, cacheId, clientVersion, rw) {
		response(rw, nil, cache.ChangeType_Unchange, 0, nil, 13, errors.New("set cache error"), this.logger)
	} else {
		//this.next.ServeHTTP(rw, req)
	}
}

// 读取缓存内容，return success
func (this *grpcHandle) getCache(locker *sync.RWMutex, isIncr bool, cacheId string, clientVersion int64, rw http.ResponseWriter) bool {
	locker.RLock()
	defer locker.RUnlock()
	if isIncr {
		c, ct, change, version, hit := this.cacheManager.GetVersionCache(cacheId, clientVersion)
		if hit {
			cacheBytes, err := c.Marshal()
			if err != nil {
				response(rw, nil, cache.ChangeType_Unchange, 0, nil, 13, err, this.logger)
				return false
			}
			this.logger.Debug("body: ")
			this.logger.Debug(c)
			response(rw, cacheBytes, ct, version, change, 0, nil, this.logger)
			return true
		}
	} else {
		bts, hit := this.cacheManager.GetNoVersionCache(cacheId, time.Now().Unix())
		if hit {
			response(rw, bts[5:], cache.ChangeType_Create, 1, nil, 0, nil, this.logger)
			return true
		}
	}
	return false
}

func (this *grpcHandle) setCache(cacheId string, locker *sync.RWMutex, reqBytes []byte, req *http.Request, rw http.ResponseWriter, methodDesc *desc.MethodDescriptor, a *grpcHandle, isIncr bool, logger log.Logger) (int, string) {
	locker.Lock()
	defer locker.Unlock()
	req.Body = ioutil.NopCloser(bytes.NewReader(reqBytes))
	newRw := &cacheResponse{
		cacheId: "",
		buffer:  &bytes.Buffer{},
		header:  rw.Header(),
	}
	this.next.ServeHTTP(newRw, req)
	protoVersion := newRw.Header().Get("Proto-Version")
	if protoVersion != this.currentVersion {
		return 503, ""
	}
	grpcStatus, _ := strconv.Atoi(newRw.header.Get("Grpc-Status"))
	if grpcStatus != 0 {
		return grpcStatus, newRw.header.Get("Grpc-Message")
	}
	if isIncr {
		msg := dynamicProto.NewMessage(methodDesc.GetOutputType())
		err := msg.Unmarshal(newRw.buffer.Bytes()[5:])
		if err != nil {
			this.logger.Error("can't parse response bytes to message.")
			return 3, "can't parse response bytes to message."
		}
		this.cacheManager.SetVersionCache(cacheId, time.Now().Unix(), msg, methodDesc.GetOutputType(), a.ttl*1000, a.maxVersionCount)
	} else {
		this.cacheManager.SetNoVersionCache(cacheId, newRw.buffer.Bytes(), a.ttl*1000)
	}
	return 0, ""
}

func response(rw http.ResponseWriter, resultBts []byte, ct cache.ChangeType, version int64, change *vfasys.ChangeDesc, code int, err error, logger log.Logger) {
	buffer := &bytes.Buffer{}
	buffer.Write([]byte{0})
	length := make([]byte, 4)
	if resultBts != nil {
		binary.BigEndian.PutUint32(length, uint32(len(resultBts)))
	} else {
		binary.BigEndian.PutUint32(length, 0)
	}
	buffer.Write(length)
	buffer.Write(resultBts)
	//chunk := httputil.NewChunkedWriter(rw)
	rw.Header().Set("Content-Type", "application/grpc")
	rw.Header().Set("Grpc-Accept-Encoding", "gzip")
	rw.Header().Set("Grpc-Encoding", "identity")
	rw.Header().Set("Trailer:Grpc-Status", strconv.Itoa(code))
	rw.Header().Set("ts", strconv.FormatInt(version, 10))
	rw.Header().Set("Vary", "Origin")
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	if change != nil {
		cmBts, err := proto.Marshal(change)
		if err != nil {
			if code == 5 {
				rw.WriteHeader(404)
			} else if code == 13 {
				rw.WriteHeader(500)
			} else if code == 0 {
				rw.WriteHeader(200)
			} else if code == 12 {
				rw.WriteHeader(500)
			}
			return
		}
		logger.Debug("cd-bin ")
		logger.Debug(proto.MarshalTextString(change))
		rw.Header().Set("cd-bin", base64.StdEncoding.EncodeToString(cmBts))
	}
	if err != nil {
		rw.Header().Set("rs", err.Error())
	}
	logger.Debug("ct-bin ")
	logger.Debug(ct)
	rw.Header().Set("ct-bin", base64.StdEncoding.EncodeToString([]byte{byte(ct)}))
	rw.Header().Set("Transfer-Encoding", "chunked")
	rw.WriteHeader(200)
	rw.Write(buffer.Bytes())
	rw.(http.Flusher).Flush()
	rw.WriteHeader(200)
}

type cacheResponse struct {
	cacheId string
	buffer  *bytes.Buffer
	code    int
	header  http.Header
}

func (r *cacheResponse) Header() http.Header {
	return r.header
}

func (r *cacheResponse) Write(bts []byte) (int, error) {
	return r.buffer.Write(bts)
}

func (r *cacheResponse) WriteHeader(statusCode int) {
	r.code = statusCode
}
