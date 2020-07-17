package grpchandle

import (
	"bytes"
	"compress/zlib"
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
	dataservice "github.com/containous/traefik/v2/pkg/cache/proto/sys"
	"github.com/containous/traefik/v2/pkg/config/dynamic"
	"github.com/containous/traefik/v2/pkg/log"
	"github.com/containous/traefik/v2/pkg/middlewares"
	"github.com/containous/traefik/v2/pkg/tracing"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/jhump/protoreflect/desc"
	dynamicProto "github.com/jhump/protoreflect/dynamic"
	"github.com/mwitkow/grpc-proxy/proxy"
	"github.com/opentracing/opentracing-go/ext"
	gca "github.com/patrickmn/go-cache"
	_ "github.com/vulcand/oxy/buffer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
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

	rpcs            map[string]*desc.MethodDescriptor
	cache           bool
	ttl             time.Duration
	maxVersionCount int
	incrRegex       *regexp.Regexp
	fullRegex       *regexp.Regexp
	lockers         *gca.Cache

	backendPool *GRPCConnectionPool
}

// New creates a new handler.
func New(ctx context.Context, next http.Handler, config dynamic.GRPCHandler, name string, backends []string) (http.Handler, error) {
	logger := log.FromContext(middlewares.GetLoggerCtx(ctx, name, typeName))
	logger.Debug("Creating middleware")
	incrReg, err := regexp.Compile(config.IncrRegex)
	if err != nil {
		logger.Fatal("parse regex `" + config.IncrRegex + "` faield. " + err.Error())
	}
	fullReg, err := regexp.Compile(config.FullRegex)
	if err != nil {
		logger.Fatal("parse regex `" + config.FullRegex + "` faield. " + err.Error())
	}

	result := &grpcHandle{
		next:      next,
		name:      name,
		messages:  map[string]*desc.MessageDescriptor{},
		rpcs:      map[string]*desc.MethodDescriptor{},
		cache:     config.Cache,
		ttl:       time.Second * time.Duration(config.TTL),
		incrRegex: incrReg,
		fullRegex: fullReg,
		lockers:   gca.New(time.Hour*10, time.Minute*10),
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
	result.backendPool = NewPool(backendURLs)

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
		conn, err := result.backendPool.Get()
		if err != nil {
			logger.Error(err)
			return nil, nil, err
		}
		return outCtx, conn, nil
	}
	result.grpc = grpc.NewServer(grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
		grpc.CustomCodec(proxy.Codec()),
		grpc.MaxRecvMsgSize(1024*1024*4),
	)
	result.websocketWrapper = grpcweb.WrapServer(result.grpc, grpcweb.WithWebsockets(true), grpcweb.WithCorsForRegisteredEndpointsOnly(false), grpcweb.WithOriginFunc(func(origin string) bool {
		return true
	}), grpcweb.WithWebsocketOriginFunc(func(req *http.Request) bool {
		return true
	}), grpcweb.WithWebsocketPingInterval(time.Second*5), grpcweb.WithAllowedRequestHeaders([]string{"*"}))

	if config.MaxVersionCount == 0 {
		result.maxVersionCount = 200
	}

	wrapperFs := new(descriptor.FileDescriptorSet)
	wrapperByte, _ := base64.StdEncoding.DecodeString("Ct0BChlnb29nbGUvcHJvdG9idWYvYW55LnByb3RvEg9nb29nbGUucHJvdG9idWYiNgoDQW55EhkKCHR5cGVfdXJsGAEgASgJUgd0eXBlVXJsEhQKBXZhbHVlGAIgASgMUgV2YWx1ZUJvChNjb20uZ29vZ2xlLnByb3RvYnVmQghBbnlQcm90b1ABWiVnaXRodWIuY29tL2dvbGFuZy9wcm90b2J1Zi9wdHlwZXMvYW55ogIDR1BCqgIeR29vZ2xlLlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMK8wEKHmdvb2dsZS9wcm90b2J1Zi9kdXJhdGlvbi5wcm90bxIPZ29vZ2xlLnByb3RvYnVmIjoKCER1cmF0aW9uEhgKB3NlY29uZHMYASABKANSB3NlY29uZHMSFAoFbmFub3MYAiABKAVSBW5hbm9zQnwKE2NvbS5nb29nbGUucHJvdG9idWZCDUR1cmF0aW9uUHJvdG9QAVoqZ2l0aHViLmNvbS9nb2xhbmcvcHJvdG9idWYvcHR5cGVzL2R1cmF0aW9u+AEBogIDR1BCqgIeR29vZ2xlLlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMKtwEKG2dvb2dsZS9wcm90b2J1Zi9lbXB0eS5wcm90bxIPZ29vZ2xlLnByb3RvYnVmIgcKBUVtcHR5QnYKE2NvbS5nb29nbGUucHJvdG9idWZCCkVtcHR5UHJvdG9QAVonZ2l0aHViLmNvbS9nb2xhbmcvcHJvdG9idWYvcHR5cGVzL2VtcHR5+AEBogIDR1BCqgIeR29vZ2xlLlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMK7QEKIGdvb2dsZS9wcm90b2J1Zi9maWVsZF9tYXNrLnByb3RvEg9nb29nbGUucHJvdG9idWYiIQoJRmllbGRNYXNrEhQKBXBhdGhzGAEgAygJUgVwYXRoc0KMAQoTY29tLmdvb2dsZS5wcm90b2J1ZkIORmllbGRNYXNrUHJvdG9QAVo5Z29vZ2xlLmdvbGFuZy5vcmcvZ2VucHJvdG8vcHJvdG9idWYvZmllbGRfbWFzaztmaWVsZF9tYXNr+AEBogIDR1BCqgIeR29vZ2xlLlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMKhQIKJGdvb2dsZS9wcm90b2J1Zi9zb3VyY2VfY29udGV4dC5wcm90bxIPZ29vZ2xlLnByb3RvYnVmIiwKDVNvdXJjZUNvbnRleHQSGwoJZmlsZV9uYW1lGAEgASgJUghmaWxlTmFtZUKVAQoTY29tLmdvb2dsZS5wcm90b2J1ZkISU291cmNlQ29udGV4dFByb3RvUAFaQWdvb2dsZS5nb2xhbmcub3JnL2dlbnByb3RvL3Byb3RvYnVmL3NvdXJjZV9jb250ZXh0O3NvdXJjZV9jb250ZXh0ogIDR1BCqgIeR29vZ2xlLlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMK5QUKHGdvb2dsZS9wcm90b2J1Zi9zdHJ1Y3QucHJvdG8SD2dvb2dsZS5wcm90b2J1ZiKYAQoGU3RydWN0EjsKBmZpZWxkcxgBIAMoCzIjLmdvb2dsZS5wcm90b2J1Zi5TdHJ1Y3QuRmllbGRzRW50cnlSBmZpZWxkcxpRCgtGaWVsZHNFbnRyeRIQCgNrZXkYASABKAlSA2tleRIsCgV2YWx1ZRgCIAEoCzIWLmdvb2dsZS5wcm90b2J1Zi5WYWx1ZVIFdmFsdWU6AjgBIrICCgVWYWx1ZRI7CgpudWxsX3ZhbHVlGAEgASgOMhouZ29vZ2xlLnByb3RvYnVmLk51bGxWYWx1ZUgAUgludWxsVmFsdWUSIwoMbnVtYmVyX3ZhbHVlGAIgASgBSABSC251bWJlclZhbHVlEiMKDHN0cmluZ192YWx1ZRgDIAEoCUgAUgtzdHJpbmdWYWx1ZRIfCgpib29sX3ZhbHVlGAQgASgISABSCWJvb2xWYWx1ZRI8CgxzdHJ1Y3RfdmFsdWUYBSABKAsyFy5nb29nbGUucHJvdG9idWYuU3RydWN0SABSC3N0cnVjdFZhbHVlEjsKCmxpc3RfdmFsdWUYBiABKAsyGi5nb29nbGUucHJvdG9idWYuTGlzdFZhbHVlSABSCWxpc3RWYWx1ZUIGCgRraW5kIjsKCUxpc3RWYWx1ZRIuCgZ2YWx1ZXMYASADKAsyFi5nb29nbGUucHJvdG9idWYuVmFsdWVSBnZhbHVlcyobCglOdWxsVmFsdWUSDgoKTlVMTF9WQUxVRRAAQoEBChNjb20uZ29vZ2xlLnByb3RvYnVmQgtTdHJ1Y3RQcm90b1ABWjFnaXRodWIuY29tL2dvbGFuZy9wcm90b2J1Zi9wdHlwZXMvc3RydWN0O3N0cnVjdHBi+AEBogIDR1BCqgIeR29vZ2xlLlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMK9wEKH2dvb2dsZS9wcm90b2J1Zi90aW1lc3RhbXAucHJvdG8SD2dvb2dsZS5wcm90b2J1ZiI7CglUaW1lc3RhbXASGAoHc2Vjb25kcxgBIAEoA1IHc2Vjb25kcxIUCgVuYW5vcxgCIAEoBVIFbmFub3NCfgoTY29tLmdvb2dsZS5wcm90b2J1ZkIOVGltZXN0YW1wUHJvdG9QAVorZ2l0aHViLmNvbS9nb2xhbmcvcHJvdG9idWYvcHR5cGVzL3RpbWVzdGFtcPgBAaICA0dQQqoCHkdvb2dsZS5Qcm90b2J1Zi5XZWxsS25vd25UeXBlc2IGcHJvdG8zCv4DCh5nb29nbGUvcHJvdG9idWYvd3JhcHBlcnMucHJvdG8SD2dvb2dsZS5wcm90b2J1ZiIjCgtEb3VibGVWYWx1ZRIUCgV2YWx1ZRgBIAEoAVIFdmFsdWUiIgoKRmxvYXRWYWx1ZRIUCgV2YWx1ZRgBIAEoAlIFdmFsdWUiIgoKSW50NjRWYWx1ZRIUCgV2YWx1ZRgBIAEoA1IFdmFsdWUiIwoLVUludDY0VmFsdWUSFAoFdmFsdWUYASABKARSBXZhbHVlIiIKCkludDMyVmFsdWUSFAoFdmFsdWUYASABKAVSBXZhbHVlIiMKC1VJbnQzMlZhbHVlEhQKBXZhbHVlGAEgASgNUgV2YWx1ZSIhCglCb29sVmFsdWUSFAoFdmFsdWUYASABKAhSBXZhbHVlIiMKC1N0cmluZ1ZhbHVlEhQKBXZhbHVlGAEgASgJUgV2YWx1ZSIiCgpCeXRlc1ZhbHVlEhQKBXZhbHVlGAEgASgMUgV2YWx1ZUJ8ChNjb20uZ29vZ2xlLnByb3RvYnVmQg1XcmFwcGVyc1Byb3RvUAFaKmdpdGh1Yi5jb20vZ29sYW5nL3Byb3RvYnVmL3B0eXBlcy93cmFwcGVyc/gBAaICA0dQQqoCHkdvb2dsZS5Qcm90b2J1Zi5XZWxsS25vd25UeXBlc2IGcHJvdG8z")
	wrapperFs.XXX_Unmarshal(wrapperByte)

	fs := new(descriptor.FileDescriptorSet)
	descBytes, _ := base64.StdEncoding.DecodeString(config.Desc)
	r, _ := zlib.NewReader(bytes.NewReader(descBytes))
	buf := bytes.NewBuffer([]byte{})
	io.Copy(buf, r)
	err = fs.XXX_Unmarshal(buf.Bytes())
	if err != nil {
		logger.Fatal("can't parse desc")
	}
	fs.File = append(fs.File, wrapperFs.File...)
	files, err := desc.CreateFileDescriptorsFromSet(fs)
	if err != nil {
		logger.Panic(err)
	}
	for _, file := range files {
		for _, msgType := range file.GetMessageTypes() {
			result.messages[msgType.GetFullyQualifiedName()] = msgType
		}
		for _, service := range file.GetServices() {
			for _, method := range service.GetMethods() {
				if len(file.GetPackage()) > 0 {
					result.rpcs[fmt.Sprintf("/%s.%s/%s", file.GetPackage(), service.GetName(), method.GetName())] = method
				} else {
					result.rpcs[fmt.Sprintf("/%s/%s", service.GetName(), method.GetName())] = method
				}
			}
		}
	}

	return result, nil
}

func handle(srv interface{}, stream grpc.ServerStream) error {
	print(stream.Context().Value("st"))
	return nil
}

func (a *grpcHandle) GetTracingInformation() (string, ext.SpanKindEnum) {
	return a.name, tracing.SpanKindNoneEnum
}

func (a *grpcHandle) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	// TODO: debug
	logger := log.FromContext(middlewares.GetLoggerCtx(req.Context(), a.name, typeName))
	if a.websocketWrapper.IsGrpcWebSocketRequest(req) {
		a.websocketWrapper.HandleGrpcWebsocketRequest(rw, req)
		//a.next.ServeHTTP(rw, req)
		return
	}
	if req.ProtoMajor != 2 || !strings.Contains(req.Header.Get("Content-Type"), "application/grpc") {
		a.next.ServeHTTP(rw, req)
		return
	}
	isFullCache := a.fullRegex.MatchString(req.RequestURI)
	isIncrCache := a.incrRegex.MatchString(req.RequestURI)
	if !a.cache || (!isFullCache && !isIncrCache) {
		a.next.ServeHTTP(rw, req)
		return
	}
	var clientVersion int64
	if req.Header.Get("ts") == "" {
		response(rw, nil, cache.ChangeType_Unchange, 0, nil, 5, errors.New("miss ts header"))
		return
	}
	if v, err := strconv.ParseInt(req.Header.Get("ts"), 10, 64); err != nil {
		response(rw, nil, cache.ChangeType_Unchange, 0, nil, 5, errors.New("ts header not int64"))
	} else {
		clientVersion = v
	}

	methodDesc := a.rpcs[req.RequestURI]
	if methodDesc == nil {
		response(rw, nil, cache.ChangeType_Unchange, 0, nil, 5, errors.New("request URI "+req.RequestURI+" is not found"))
		return
	}
	bts, _ := ioutil.ReadAll(req.Body)
	if len(bts) < 5 {
		response(rw, nil, cache.ChangeType_Unchange, 0, nil, 5, errors.New("parse body to gRPC failed"))
		return
	}
	frame := bts[5:]
	cacheIdBuffer := &bytes.Buffer{}
	cacheIdBuffer.Write([]byte(req.RequestURI))
	cacheIdBuffer.Write(frame)
	h := md5.New()
	h.Write(cacheIdBuffer.Bytes())
	cacheId := hex.EncodeToString(h.Sum(nil))
	locker, exists := a.lockers.Get(cacheId)
	if !exists {
		locker = &sync.RWMutex{}
		a.lockers.Add(cacheId, locker, 0)
	}
	// 成功命中则写入并终止处理
	if getCache(locker.(*sync.RWMutex), isIncrCache, cacheId, clientVersion, rw) {
		//a.next.ServeHTTP(rw, req)
		return
	}

	code, msg := setCache(cacheId, locker.(*sync.RWMutex), bts, req, methodDesc, a, isIncrCache, logger)
	if code != 0 {
		response(rw, nil, cache.ChangeType_Unchange, 0, nil, code, errors.New(msg))
		return
	}
	if !getCache(locker.(*sync.RWMutex), isIncrCache, cacheId, clientVersion, rw) {
		response(rw, nil, cache.ChangeType_Unchange, 0, nil, 13, errors.New("set cache error"))
	} else {
		//a.next.ServeHTTP(rw, req)
	}
}

// 读取缓存内容，return success
func getCache(locker *sync.RWMutex, isIncr bool, cacheId string, clientVersion int64, rw http.ResponseWriter) bool {
	locker.RLock()
	defer locker.RUnlock()
	if isIncr {
		c, ct, change, version, hit := cache.CacheManager.GetVersionCache(cacheId, clientVersion)
		if hit {
			cacheBytes, err := c.Marshal()
			if err != nil {
				response(rw, nil, cache.ChangeType_Unchange, 0, nil, 13, err)
				return false
			}
			response(rw, cacheBytes, ct, version, change, 0, nil)
			return true
		}
	} else {
		cache, hit := cache.CacheManager.GetNoVersionCache(cacheId, time.Now().Unix())
		if hit {
			rw.Write(cache)
			rw.Header().Add("Trailer:Grpc-Status", "0")
			rw.WriteHeader(200)
			return true
		}
	}
	return false
}

func response(rw http.ResponseWriter, resultBts []byte, ct cache.ChangeType, version int64, change *dataservice.ChangeDesc, code int, err error) {
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
	rw.Header().Add("Content-Type", "application/grpc")
	rw.Header().Add("Grpc-Accept-Encoding", "gzip")
	rw.Header().Add("Grpc-Encoding", "identity")
	rw.Header().Add("Trailer:Grpc-Status", strconv.Itoa(code))
	rw.Header().Add("ts", strconv.FormatInt(version, 10))
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
		rw.Header().Add("cd-bin", base64.StdEncoding.EncodeToString(cmBts))
	}
	if err != nil {
		rw.Header().Add("rs", err.Error())
	}
	rw.Header().Add("ct-bin", base64.StdEncoding.EncodeToString([]byte{byte(ct)}))
	rw.WriteHeader(200)
	rw.Write(buffer.Bytes())
}

func setCache(cacheId string, locker *sync.RWMutex, reqBytes []byte, req *http.Request, methodDesc *desc.MethodDescriptor, a *grpcHandle, isIncr bool, logger log.Logger) (int, string) {
	locker.Lock()
	defer locker.Unlock()
	req.Body = ioutil.NopCloser(bytes.NewReader(reqBytes))
	newRw := &cacheResponse{
		cacheId: "",
		buffer:  &bytes.Buffer{},
		header:  map[string][]string{},
	}
	a.next.ServeHTTP(newRw, req)
	grpcStatus, _ := strconv.Atoi(newRw.header.Get("Grpc-Status"))
	if grpcStatus != 0 {
		return grpcStatus, newRw.header.Get("Grpc-Message")
	}
	if isIncr {
		msg := dynamicProto.NewMessage(methodDesc.GetOutputType())
		err := msg.Unmarshal(newRw.buffer.Bytes()[5:])
		if err != nil {
			logger.Errorf("can't parse response bytes to message.")
			return 3, "can't parse response bytes to message."
		}
		cache.CacheManager.SetVersionCache(cacheId, time.Now().Unix(), msg, methodDesc.GetOutputType(), a.ttl.Milliseconds(), a.maxVersionCount)
	} else {
		cache.CacheManager.SetNoVersionCache(cacheId, newRw.buffer.Bytes(), a.ttl.Milliseconds())
	}
	return 0, ""
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
