package grpchandle

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	_ "encoding/binary"
	"fmt"
	"github.com/containous/traefik/v2/pkg/cache"
	_ "github.com/containous/traefik/v2/pkg/cache"
	"github.com/containous/traefik/v2/pkg/config/dynamic"
	"github.com/containous/traefik/v2/pkg/log"
	"github.com/containous/traefik/v2/pkg/middlewares"
	"github.com/containous/traefik/v2/pkg/tracing"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	dynamicProto "github.com/jhump/protoreflect/dynamic"
	"github.com/opentracing/opentracing-go/ext"
	gca "github.com/patrickmn/go-cache"
	_ "github.com/vulcand/oxy/buffer"
	"google.golang.org/grpc"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	typeName = "gRPCHandle"
)

type grpcHandle struct {
	next     http.Handler
	name     string
	grpc     *grpc.Server
	messages map[string]*desc.MessageDescriptor

	rpcs            map[string]*desc.MethodDescriptor
	cache           bool
	ttl             time.Duration
	incremental     bool
	maxVersionCount int
	lockers         *gca.Cache
}

// New creates a new handler.
func New(ctx context.Context, next http.Handler, config dynamic.GRPCHandler, name string) (http.Handler, error) {
	logger := log.FromContext(middlewares.GetLoggerCtx(ctx, name, typeName))
	logger.Debug("Creating middleware")
	result := &grpcHandle{
		next:        next,
		name:        name,
		grpc:        grpc.NewServer(grpc.UnknownServiceHandler(handle)),
		messages:    map[string]*desc.MessageDescriptor{},
		rpcs:        map[string]*desc.MethodDescriptor{},
		cache:       config.Cache,
		ttl:         time.Millisecond * time.Duration(config.TTL),
		incremental: config.Incremental,
		lockers:     gca.New(time.Hour*10, time.Minute*10),
	}
	if config.MaxVersionCount == 0 {
		result.maxVersionCount = 200
	}

	wrapperFs := new(descriptor.FileDescriptorSet)
	wrapperByte, _ := base64.StdEncoding.DecodeString("Cv4DCh5nb29nbGUvcHJvdG9idWYvd3JhcHBlcnMucHJvdG8SD2dvb2dsZS5wcm90b2J1ZiIjCgtEb3VibGVWYWx1ZRIUCgV2YWx1ZRgBIAEoAVIFdmFsdWUiIgoKRmxvYXRWYWx1ZRIUCgV2YWx1ZRgBIAEoAlIFdmFsdWUiIgoKSW50NjRWYWx1ZRIUCgV2YWx1ZRgBIAEoA1IFdmFsdWUiIwoLVUludDY0VmFsdWUSFAoFdmFsdWUYASABKARSBXZhbHVlIiIKCkludDMyVmFsdWUSFAoFdmFsdWUYASABKAVSBXZhbHVlIiMKC1VJbnQzMlZhbHVlEhQKBXZhbHVlGAEgASgNUgV2YWx1ZSIhCglCb29sVmFsdWUSFAoFdmFsdWUYASABKAhSBXZhbHVlIiMKC1N0cmluZ1ZhbHVlEhQKBXZhbHVlGAEgASgJUgV2YWx1ZSIiCgpCeXRlc1ZhbHVlEhQKBXZhbHVlGAEgASgMUgV2YWx1ZUJ8ChNjb20uZ29vZ2xlLnByb3RvYnVmQg1XcmFwcGVyc1Byb3RvUAFaKmdpdGh1Yi5jb20vZ29sYW5nL3Byb3RvYnVmL3B0eXBlcy93cmFwcGVyc/gBAaICA0dQQqoCHkdvb2dsZS5Qcm90b2J1Zi5XZWxsS25vd25UeXBlc2IGcHJvdG8z")
	wrapperFs.XXX_Unmarshal(wrapperByte)

	fs := new(descriptor.FileDescriptorSet)
	descBytes, _ := base64.StdEncoding.DecodeString(config.Desc)
	err := fs.XXX_Unmarshal(descBytes)
	if err != nil {
		logger.Error("can't parse desc")
	}
	fs.File = append(fs.File, wrapperFs.File...)
	files, _ := desc.CreateFileDescriptorsFromSet(fs)
	for _, file := range files {
		for _, msgType := range file.GetMessageTypes() {
			result.messages[msgType.GetFullyQualifiedName()] = msgType
		}
		for _, service := range file.GetServices() {
			for _, method := range service.GetMethods() {
				result.rpcs[fmt.Sprintf("/%s.%s/%s", file.GetPackage(), service.GetName(), method.GetName())] = method
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
	logger := log.FromContext(middlewares.GetLoggerCtx(req.Context(), a.name, typeName))
	if req.ProtoMajor != 2 || !strings.Contains(req.Header.Get("Content-Type"), "application/grpc") {
		a.next.ServeHTTP(rw, req)
		return
	}
	if !a.cache {
		a.next.ServeHTTP(rw, req)
		return
	}
	var clientVersion int64
	if req.Header.Get("ts") == "" {
		rw.Write([]byte("miss ts field in request headers."))
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	if v, err := strconv.ParseInt(req.Header.Get("ts"), 10, 64); err != nil {
		rw.WriteHeader(http.StatusBadRequest)
	} else {
		clientVersion = v
	}

	methodDesc := a.rpcs[req.RequestURI]
	if methodDesc == nil {
		rw.WriteHeader(404)
		return
	}
	bts, _ := ioutil.ReadAll(req.Body)
	if len(bts) < 5 {
		rw.WriteHeader(400)
		return
	}
	frame := bts[5:]
	msg := dynamicProto.NewMessage(methodDesc.GetInputType())
	err := msg.Unmarshal(frame)
	if err != nil {
		rw.WriteHeader(400)
		return
	}
	cacheIdBuffer := &bytes.Buffer{}
	cacheIdBuffer.Write([]byte(req.RequestURI))
	cacheIdBuffer.Write(frame)
	cacheId := base64.StdEncoding.EncodeToString(cacheIdBuffer.Bytes())
	locker, exists := a.lockers.Get(cacheId)
	if !exists {
		locker = &sync.RWMutex{}
		a.lockers.Add(cacheId, locker, 0)
	}
	if readCache(locker.(*sync.RWMutex), a, cacheId, clientVersion, rw) {
		a.next.ServeHTTP(rw, req)
		return
	}
	newRw := &cacheResponse{
		cacheId: "",
		buffer:  &bytes.Buffer{},
		header:  map[string][]string{},
	}

	writeCache(cacheId, locker.(*sync.RWMutex), bts, req, newRw, methodDesc, a, logger)
	if !readCache(locker.(*sync.RWMutex), a, cacheId, clientVersion, rw) {
		rw.WriteHeader(newRw.code)
		rw.Write([]byte("俺也不知道"))
	} else {
		//a.next.ServeHTTP(rw, req)
	}
}

func readCache(locker *sync.RWMutex, a *grpcHandle, cacheId string, clientVersion int64, rw http.ResponseWriter) bool {
	locker.RLock()
	defer locker.RUnlock()
	if a.incremental {
		cache, ct, change, version, hit := cache.CacheManager.GetVersionCache(cacheId, clientVersion)
		if hit {
			cacheBytes, err := cache.Marshal()
			if err != nil {
				rw.WriteHeader(500)
				return false
			}
			cmBts, err := proto.Marshal(change)
			if err != nil {
				rw.WriteHeader(500)
				return false
			}
			buffer := &bytes.Buffer{}
			buffer.Write([]byte{0})
			length := make([]byte, 4)
			binary.BigEndian.PutUint32(length, uint32(len(cacheBytes)))
			buffer.Write(length)
			buffer.Write(cacheBytes)
			rw.Header().Add("Content-Type", "application/grpc")
			rw.Header().Add("Grpc-Accept-Encoding", "gzip")
			rw.Header().Add("Grpc-Encoding", "identity")
			rw.Header().Add("Trailer:Grpc-Status", "0")
			rw.Header().Add("ts", strconv.FormatInt(version, 10))
			rw.Header().Add("cm", base64.StdEncoding.EncodeToString(cmBts))
			rw.Header().Add("ct", fmt.Sprintf("%x", ct))
			rw.WriteHeader(200)
			rw.Write(buffer.Bytes())
			return true
		}
	} else {
		cache, hit := cache.CacheManager.GetNoVersionCache(cacheId, time.Now().Unix())
		if hit {
			rw.Write(cache)
			rw.WriteHeader(200)
			return true
		}
	}
	return false
}

func writeCache(cacheId string, locker *sync.RWMutex, reqBytes []byte, req *http.Request, newRw *cacheResponse, methodDesc *desc.MethodDescriptor, a *grpcHandle, logger log.Logger) {
	locker.Lock()
	defer locker.Unlock()
	req.Body = ioutil.NopCloser(bytes.NewReader(reqBytes))

	a.next.ServeHTTP(newRw, req)

	if a.incremental {
		msg := dynamicProto.NewMessage(methodDesc.GetOutputType())
		err := msg.Unmarshal(newRw.buffer.Bytes()[5:])
		if err != nil {
			logger.Errorf("can't parse response bytes to message.")
			return
		}
		cache.CacheManager.SetVersionCache(cacheId, time.Now().Unix(), msg, methodDesc.GetOutputType(), a.ttl.Milliseconds())
	} else {
		cache.CacheManager.SetNoVersionCache(cacheId, newRw.buffer.Bytes(), a.ttl.Milliseconds())
	}
}

type cacheResponse struct {
	cacheId string
	buffer  *bytes.Buffer
	code    int
	header  http.Header
}

func (r cacheResponse) Header() http.Header {
	return r.header
}

func (r cacheResponse) Write(bts []byte) (int, error) {
	r.buffer.Write(bts)
	return len(bts), nil
}

func (r cacheResponse) WriteHeader(statusCode int) {
	r.code = statusCode
}
