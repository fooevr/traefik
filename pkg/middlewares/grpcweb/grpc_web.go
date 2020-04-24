package grpcweb

import (
	"context"
	"github.com/containous/traefik/v2/pkg/config/dynamic"
	"github.com/containous/traefik/v2/pkg/tracing"
	"github.com/opentracing/opentracing-go/ext"
	"net/http"
)

const typeName = "grpcWeb"

type grpcWeb struct {
	name   string
	ctx    context.Context
	next   http.Handler
	config dynamic.GRPCWeb
}

func New(ctx context.Context, next http.Handler, config dynamic.GRPCWeb, name string) (http.Handler, error) {
	return &grpcWeb{
		name:   name,
		ctx:    ctx,
		next:   next,
		config: config,
	}, nil
}

func (a *grpcWeb) GetTracingInformation() (string, ext.SpanKindEnum) {
	return a.name, tracing.SpanKindNoneEnum
}

func (a *grpcWeb) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodOptions {
		a.next.ServeHTTP(rw, req)
		return
	}

	a.next.ServeHTTP(rw, req)
}
