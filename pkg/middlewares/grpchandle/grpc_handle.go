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
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/codes"
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
	fs := new(descriptor.FileDescriptorSet)
	descBytes, _ := base64.StdEncoding.DecodeString(config.Desc)
	err := fs.XXX_Unmarshal(descBytes)
	if err != nil {
		logger.Error("can't parse desc")
	}
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
	cacheId := base64.StdEncoding.EncodeToString(frame)
	locker, exists := a.lockers.Get(cacheId)
	if !exists {
		locker := &sync.RWMutex{}
		a.lockers.Add(cacheId, locker, 0)
	}
	readCache := func() bool {
		locker.(*sync.RWMutex).RLock()
		defer locker.(*sync.RWMutex).RUnlock()
		if a.incremental {
			cache, change, version, hit := cache.CacheManager.GetVersionCache(cacheId, clientVersion)
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
				binary.BigEndian.PutUint32(length, uint32(len(cacheBytes)-5))
				buffer.Write(length)
				buffer.Write(cacheBytes)
				rw.WriteHeader(200)
				rw.Write(buffer.Bytes())
				rw.Header().Add("Content-Type", "application/grpc")
				rw.Header().Add("Grpc-Accept-Encoding", "gzip")
				rw.Header().Add("Grpc-Encoding", "identity")
				rw.Header().Add("Trailer:Grpc-Status", codes.OK.String())
				rw.Header().Add("ts", strconv.FormatInt(version, 10))
				rw.Header().Add("cm", base64.StdEncoding.EncodeToString(cmBts))
				a.next.ServeHTTP(rw, req)
				return true
			}
		} else {
			cache, hit := cache.CacheManager.GetNoVersionCache(cacheId, time.Now().Unix())
			if hit {
				rw.Write(cache)
				rw.WriteHeader(200)
				a.next.ServeHTTP(rw, req)
				return true
			}
		}
		return false
	}
	if readCache() {
		return
	}
	func() {
		locker.(*sync.RWMutex).Lock()
		defer locker.(*sync.RWMutex).Unlock()
		req.Body = ioutil.NopCloser(bytes.NewReader(bts))
		newRw := cacheResponse{
			cacheId:      "",
			sourceWriter: rw,
			buffer:       &bytes.Buffer{},
		}
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
	}()
	readCache()
}

type cacheResponse struct {
	cacheId      string
	sourceWriter http.ResponseWriter
	buffer       *bytes.Buffer
	code         int
}

func (r cacheResponse) Header() http.Header {
	return r.sourceWriter.Header()
}

func (r cacheResponse) Write(bts []byte) (int, error) {
	r.buffer.Write(bts)
	return r.sourceWriter.Write(bts)
}

func (r cacheResponse) WriteHeader(statusCode int) {
	r.code = statusCode
	r.sourceWriter.WriteHeader(statusCode)
}
