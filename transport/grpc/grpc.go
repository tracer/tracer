package grpc

import (
	"errors"
	"net"

	"github.com/tracer/tracer"
	"github.com/tracer/tracer/pb"
	"github.com/tracer/tracer/pbutil"
	"github.com/tracer/tracer/server"
	"github.com/tracer/tracer/transport"

	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func init() {
	transport.Register("grpc", setup)
}

func setup(srv *server.Server, conf map[string]interface{}) (transport.Transport, error) {
	listen, ok := conf["listen"].(string)
	if !ok {
		return nil, errors.New("missing listen setting for gRPC transport")
	}
	return &GRPC{
		srv:    srv,
		listen: listen,
	}, nil
}

type GRPC struct {
	srv    *server.Server
	listen string
}

func (g *GRPC) Start() error {
	l, err := net.Listen("tcp", g.listen)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterStorerServer(s, g)
	return s.Serve(l)
}

func (g *GRPC) Store(ctx context.Context, req *pb.StoreRequest) (*pb.StoreResponse, error) {
	st, err := pbutil.Timestamp(req.Span.StartTime)
	if err != nil {
		return nil, err
	}
	ft, err := pbutil.Timestamp(req.Span.FinishTime)
	if err != nil {
		return nil, err
	}
	sp := tracer.RawSpan{
		SpanContext: tracer.SpanContext{
			TraceID:  req.Span.TraceId,
			ParentID: req.Span.ParentId,
			SpanID:   req.Span.SpanId,
			Flags:    req.Span.Flags,
		},
		ServiceName:   req.Span.ServiceName,
		OperationName: req.Span.OperationName,
		StartTime:     st,
		FinishTime:    ft,
		Tags:          map[string]interface{}{},
	}
	for _, tag := range req.Span.Tags {
		if tag.Time != nil {
			t, err := pbutil.Timestamp(tag.Time)
			if err != nil {
				return nil, err
			}
			sp.Logs = append(sp.Logs, opentracing.LogData{
				Event:     tag.Key,
				Payload:   tag.Value,
				Timestamp: t,
			})
		} else {
			sp.Tags[tag.Key] = tag.Value
		}
	}

	err = g.srv.Storage.Store(sp)
	return &pb.StoreResponse{}, err
}
