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
	dataservice "github.com/containous/traefik/v2/pkg/cache/proto/sys"
	"github.com/containous/traefik/v2/pkg/config/dynamic"
	"github.com/containous/traefik/v2/pkg/log"
	"github.com/containous/traefik/v2/pkg/middlewares"
	"github.com/containous/traefik/v2/pkg/tracing"
	"github.com/golang/protobuf/proto"
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
	wrapperByte, _ := base64.StdEncoding.DecodeString("Ct0BChlnb29nbGUvcHJvdG9idWYvYW55LnByb3RvEg9nb29nbGUucHJvdG9idWYiNgoDQW55EhkKCHR5cGVfdXJsGAEgASgJUgd0eXBlVXJsEhQKBXZhbHVlGAIgASgMUgV2YWx1ZUJvChNjb20uZ29vZ2xlLnByb3RvYnVmQghBbnlQcm90b1ABWiVnaXRodWIuY29tL2dvbGFuZy9wcm90b2J1Zi9wdHlwZXMvYW55ogIDR1BCqgIeR29vZ2xlLlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMK8wEKHmdvb2dsZS9wcm90b2J1Zi9kdXJhdGlvbi5wcm90bxIPZ29vZ2xlLnByb3RvYnVmIjoKCER1cmF0aW9uEhgKB3NlY29uZHMYASABKANSB3NlY29uZHMSFAoFbmFub3MYAiABKAVSBW5hbm9zQnwKE2NvbS5nb29nbGUucHJvdG9idWZCDUR1cmF0aW9uUHJvdG9QAVoqZ2l0aHViLmNvbS9nb2xhbmcvcHJvdG9idWYvcHR5cGVzL2R1cmF0aW9u+AEBogIDR1BCqgIeR29vZ2xlLlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMKtwEKG2dvb2dsZS9wcm90b2J1Zi9lbXB0eS5wcm90bxIPZ29vZ2xlLnByb3RvYnVmIgcKBUVtcHR5QnYKE2NvbS5nb29nbGUucHJvdG9idWZCCkVtcHR5UHJvdG9QAVonZ2l0aHViLmNvbS9nb2xhbmcvcHJvdG9idWYvcHR5cGVzL2VtcHR5+AEBogIDR1BCqgIeR29vZ2xlLlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMK7QEKIGdvb2dsZS9wcm90b2J1Zi9maWVsZF9tYXNrLnByb3RvEg9nb29nbGUucHJvdG9idWYiIQoJRmllbGRNYXNrEhQKBXBhdGhzGAEgAygJUgVwYXRoc0KMAQoTY29tLmdvb2dsZS5wcm90b2J1ZkIORmllbGRNYXNrUHJvdG9QAVo5Z29vZ2xlLmdvbGFuZy5vcmcvZ2VucHJvdG8vcHJvdG9idWYvZmllbGRfbWFzaztmaWVsZF9tYXNr+AEBogIDR1BCqgIeR29vZ2xlLlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMKhQIKJGdvb2dsZS9wcm90b2J1Zi9zb3VyY2VfY29udGV4dC5wcm90bxIPZ29vZ2xlLnByb3RvYnVmIiwKDVNvdXJjZUNvbnRleHQSGwoJZmlsZV9uYW1lGAEgASgJUghmaWxlTmFtZUKVAQoTY29tLmdvb2dsZS5wcm90b2J1ZkISU291cmNlQ29udGV4dFByb3RvUAFaQWdvb2dsZS5nb2xhbmcub3JnL2dlbnByb3RvL3Byb3RvYnVmL3NvdXJjZV9jb250ZXh0O3NvdXJjZV9jb250ZXh0ogIDR1BCqgIeR29vZ2xlLlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMK5QUKHGdvb2dsZS9wcm90b2J1Zi9zdHJ1Y3QucHJvdG8SD2dvb2dsZS5wcm90b2J1ZiKYAQoGU3RydWN0EjsKBmZpZWxkcxgBIAMoCzIjLmdvb2dsZS5wcm90b2J1Zi5TdHJ1Y3QuRmllbGRzRW50cnlSBmZpZWxkcxpRCgtGaWVsZHNFbnRyeRIQCgNrZXkYASABKAlSA2tleRIsCgV2YWx1ZRgCIAEoCzIWLmdvb2dsZS5wcm90b2J1Zi5WYWx1ZVIFdmFsdWU6AjgBIrICCgVWYWx1ZRI7CgpudWxsX3ZhbHVlGAEgASgOMhouZ29vZ2xlLnByb3RvYnVmLk51bGxWYWx1ZUgAUgludWxsVmFsdWUSIwoMbnVtYmVyX3ZhbHVlGAIgASgBSABSC251bWJlclZhbHVlEiMKDHN0cmluZ192YWx1ZRgDIAEoCUgAUgtzdHJpbmdWYWx1ZRIfCgpib29sX3ZhbHVlGAQgASgISABSCWJvb2xWYWx1ZRI8CgxzdHJ1Y3RfdmFsdWUYBSABKAsyFy5nb29nbGUucHJvdG9idWYuU3RydWN0SABSC3N0cnVjdFZhbHVlEjsKCmxpc3RfdmFsdWUYBiABKAsyGi5nb29nbGUucHJvdG9idWYuTGlzdFZhbHVlSABSCWxpc3RWYWx1ZUIGCgRraW5kIjsKCUxpc3RWYWx1ZRIuCgZ2YWx1ZXMYASADKAsyFi5nb29nbGUucHJvdG9idWYuVmFsdWVSBnZhbHVlcyobCglOdWxsVmFsdWUSDgoKTlVMTF9WQUxVRRAAQoEBChNjb20uZ29vZ2xlLnByb3RvYnVmQgtTdHJ1Y3RQcm90b1ABWjFnaXRodWIuY29tL2dvbGFuZy9wcm90b2J1Zi9wdHlwZXMvc3RydWN0O3N0cnVjdHBi+AEBogIDR1BCqgIeR29vZ2xlLlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMK9wEKH2dvb2dsZS9wcm90b2J1Zi90aW1lc3RhbXAucHJvdG8SD2dvb2dsZS5wcm90b2J1ZiI7CglUaW1lc3RhbXASGAoHc2Vjb25kcxgBIAEoA1IHc2Vjb25kcxIUCgVuYW5vcxgCIAEoBVIFbmFub3NCfgoTY29tLmdvb2dsZS5wcm90b2J1ZkIOVGltZXN0YW1wUHJvdG9QAVorZ2l0aHViLmNvbS9nb2xhbmcvcHJvdG9idWYvcHR5cGVzL3RpbWVzdGFtcPgBAaICA0dQQqoCHkdvb2dsZS5Qcm90b2J1Zi5XZWxsS25vd25UeXBlc2IGcHJvdG8zCv4DCh5nb29nbGUvcHJvdG9idWYvd3JhcHBlcnMucHJvdG8SD2dvb2dsZS5wcm90b2J1ZiIjCgtEb3VibGVWYWx1ZRIUCgV2YWx1ZRgBIAEoAVIFdmFsdWUiIgoKRmxvYXRWYWx1ZRIUCgV2YWx1ZRgBIAEoAlIFdmFsdWUiIgoKSW50NjRWYWx1ZRIUCgV2YWx1ZRgBIAEoA1IFdmFsdWUiIwoLVUludDY0VmFsdWUSFAoFdmFsdWUYASABKARSBXZhbHVlIiIKCkludDMyVmFsdWUSFAoFdmFsdWUYASABKAVSBXZhbHVlIiMKC1VJbnQzMlZhbHVlEhQKBXZhbHVlGAEgASgNUgV2YWx1ZSIhCglCb29sVmFsdWUSFAoFdmFsdWUYASABKAhSBXZhbHVlIiMKC1N0cmluZ1ZhbHVlEhQKBXZhbHVlGAEgASgJUgV2YWx1ZSIiCgpCeXRlc1ZhbHVlEhQKBXZhbHVlGAEgASgMUgV2YWx1ZUJ8ChNjb20uZ29vZ2xlLnByb3RvYnVmQg1XcmFwcGVyc1Byb3RvUAFaKmdpdGh1Yi5jb20vZ29sYW5nL3Byb3RvYnVmL3B0eXBlcy93cmFwcGVyc/gBAaICA0dQQqoCHkdvb2dsZS5Qcm90b2J1Zi5XZWxsS25vd25UeXBlc2IGcHJvdG8z")
	wrapperFs.XXX_Unmarshal(wrapperByte)

	fs := new(descriptor.FileDescriptorSet)
	descBytes, _ := base64.StdEncoding.DecodeString(config.Desc)
	err := fs.XXX_Unmarshal(descBytes)
	if err != nil {
		logger.Error("can't parse desc")
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
		writeResult(rw, nil, cache.ChangeType_Unchange, 0, nil, 400, errors.New("miss ts header"))
		return
	}
	if v, err := strconv.ParseInt(req.Header.Get("ts"), 10, 64); err != nil {
		writeResult(rw, nil, cache.ChangeType_Unchange, 0, nil, 400, errors.New("ts header not int64"))
	} else {
		clientVersion = v
	}

	methodDesc := a.rpcs[req.RequestURI]
	if methodDesc == nil {
		writeResult(rw, nil, cache.ChangeType_Unchange, 0, nil, 404, errors.New("request URI"+req.RequestURI+" is not found"))
		return
	}
	bts, _ := ioutil.ReadAll(req.Body)
	if len(bts) < 5 {
		writeResult(rw, nil, cache.ChangeType_Unchange, 0, nil, 404, errors.New("parse body to gRPC failed"))
		return
	}
	frame := bts[5:]
	msg := dynamicProto.NewMessage(methodDesc.GetInputType())
	err := msg.Unmarshal(frame)
	if err != nil {
		writeResult(rw, nil, cache.ChangeType_Unchange, 0, nil, 400, errors.New("parse body to gRPC failed"))
		return
	}
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
	if readCache(locker.(*sync.RWMutex), a, cacheId, clientVersion, rw) {
		//a.next.ServeHTTP(rw, req)
		return
	}
	newRw := &cacheResponse{
		cacheId: "",
		buffer:  &bytes.Buffer{},
		header:  map[string][]string{},
	}

	writeCache(cacheId, locker.(*sync.RWMutex), bts, req, newRw, methodDesc, a, logger)
	if !readCache(locker.(*sync.RWMutex), a, cacheId, clientVersion, rw) {
		writeResult(rw, nil, cache.ChangeType_Unchange, 0, nil, 500, errors.New("set cache error"))
	} else {
		//a.next.ServeHTTP(rw, req)
	}
}

func readCache(locker *sync.RWMutex, a *grpcHandle, cacheId string, clientVersion int64, rw http.ResponseWriter) bool {
	locker.RLock()
	defer locker.RUnlock()
	if a.incremental {
		c, ct, change, version, hit := cache.CacheManager.GetVersionCache(cacheId, clientVersion)
		if hit {
			cacheBytes, err := c.Marshal()
			if err != nil {
				writeResult(rw, nil, cache.ChangeType_Unchange, 0, nil, 500, err)
				return false
			}
			writeResult(rw, cacheBytes, ct, version, change, 200, nil)
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

func writeResult(rw http.ResponseWriter, resultBts []byte, ct cache.ChangeType, version int64, change *dataservice.ChangeDesc, code int, err error) {
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
	rw.Header().Add("Trailer:Grpc-Status", "0")
	rw.Header().Add("ts", strconv.FormatInt(version, 10))
	if change != nil {
		cmBts, err := proto.Marshal(change)
		if err != nil {
			rw.WriteHeader(code)
			return
		}
		rw.Header().Add("cd-bin", base64.StdEncoding.EncodeToString(cmBts))
	}
	if err != nil {
		rw.Header().Add("rs", err.Error())
	}
	rw.Header().Add("ct-bin", base64.StdEncoding.EncodeToString([]byte{byte(ct)}))
	rw.WriteHeader(code)
	rw.Write(buffer.Bytes())
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
		cache.CacheManager.SetVersionCache(cacheId, time.Now().Unix(), msg, methodDesc.GetOutputType(), a.ttl.Milliseconds(), a.maxVersionCount)
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
	return r.buffer.Write(bts)
}

func (r cacheResponse) WriteHeader(statusCode int) {
	r.code = statusCode
}
