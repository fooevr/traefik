package grpchandle

import (
	"bytes"
	"context"
	"encoding/base64"
	_ "encoding/binary"
	"fmt"
	_ "github.com/containous/traefik/v2/pkg/cache"
	"github.com/containous/traefik/v2/pkg/config/dynamic"
	"github.com/containous/traefik/v2/pkg/log"
	"github.com/containous/traefik/v2/pkg/middlewares"
	"github.com/containous/traefik/v2/pkg/tracing"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	dynamicProto "github.com/jhump/protoreflect/dynamic"
	"github.com/opentracing/opentracing-go/ext"
	_ "github.com/vulcand/oxy/buffer"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/codes"
	"io/ioutil"
	"net/http"
	"strings"
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
	if req.ProtoMajor == 2 && strings.Contains(req.Header.Get("Content-Type"), "application/grpc") {
		logger.Debug(req.RequestURI)
	}
	if !a.cache {
		a.next.ServeHTTP(rw, req)
		return
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
	//cacheId := base64.StdEncoding.EncodeToString(frame)
	//cache.CacheManager.GetCahe(cacheId, a.ttl, a.incremental)
	//
	//buffer := &bytes.Buffer{}
	//buffer.Write([]byte{0})
	//length := make([]byte, 4)
	//binary.BigEndian.PutUint32(length, uint32(len(cache)-5))
	//buffer.Write(length)
	//buffer.Write(cache[5:])
	//rw.Write(buffer.Bytes())
	//rw.Header().Add("Content-Type", "application/grpc")
	//rw.Header().Add("Grpc-Accept-Encoding", "gzip")
	//rw.Header().Add("Grpc-Encoding", "identity")
	//rw.Header().Add("Trailer:Grpc-Status", codes.OK.String())
	//return

	req.Body = ioutil.NopCloser(bytes.NewReader(bts))
	newRw := cacheResponse{
		cacheId:      "",
		sourceWriter: rw,
		buffer:       &bytes.Buffer{},
	}
	a.next.ServeHTTP(newRw, req)
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
