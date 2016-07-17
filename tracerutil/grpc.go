// +build go1.7

package tracerutil

import (
	"context"
	"log"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type GRPCTextMapCarrier map[string][]string

func (g GRPCTextMapCarrier) Set(k, v string) {
	g[strings.ToLower(k)] = []string{v}
}

func (g GRPCTextMapCarrier) ForeachKey(handler func(key, val string) error) error {
	for k, v := range g {
		var vv string
		if len(v) > 0 {
			vv = v[0]
		}
		if err := handler(k, vv); err != nil {
			return err
		}
	}
	return nil
}

func NewUnaryInterceptor(tr opentracing.Tracer) func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, _ := metadata.FromContext(ctx)
		sctx, _ := tr.Extract(opentracing.TextMap, GRPCTextMapCarrier(md))
		sp := tr.StartSpan(info.FullMethod, ext.RPCServerOption(sctx))
		ext.Component.Set(sp, "grpc")

		res, err := handler(ctx, req)
		log.Println(res, err)
		if err != nil {
			ext.Error.Set(sp, true)
		}
		sp.Finish()
		return res, err
	}
}
