package server

import (
	"github.com/opentracing/opentracing-go"
	"github.com/tracer/tracer"
	"github.com/tracer/tracer/pb"
	"github.com/tracer/tracer/pbutil"

	"golang.org/x/net/context"
)

var _ pb.StorerServer = (*Server)(nil)

type Server struct {
	Storer tracer.Storer
}

func (srv *Server) Store(ctx context.Context, req *pb.StoreRequest) (*pb.StoreResponse, error) {
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

	err = srv.Storer.Store(sp)
	return &pb.StoreResponse{}, err
}
