package grpchandle

import (
	"github.com/mwitkow/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"math/rand"
	"time"
)

// TODO: advanced connection pool
type GRPCConnectionPool struct {
	backends    []string
	connections map[string]int64
	cursor      int32
}

// bk, err := grpc.Dial("127.0.0.1:8085", grpc.WithInsecure(), grpc.WithCodec(proxy.Codec()))

func NewPool(backends []string) *GRPCConnectionPool {
	return &GRPCConnectionPool{
		cursor:   0,
		backends: backends,
	}
}

func (self *GRPCConnectionPool) Get() (*grpc.ClientConn, error) {
	rand.Seed(time.Now().Unix())
	backend := self.backends[rand.Int31n(int32(len(self.backends)))]
	return grpc.Dial(backend, grpc.WithInsecure(), grpc.WithCodec(proxy.Codec()))
}

func (self *GRPCConnectionPool) Close(conn *grpc.ClientConn) {
	conn.Close()
}
