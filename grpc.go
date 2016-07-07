package tracer

import (
	"fmt"

	"github.com/tracer/tracer/pb"

	"github.com/golang/protobuf/ptypes"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type GRPC struct {
	client pb.StorerClient
}

func NewGRPC(address string, opts ...grpc.DialOption) (Storer, error) {
	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, err
	}
	client := pb.NewStorerClient(conn)
	return &GRPC{client}, nil
}

func (g *GRPC) Store(sp RawSpan) error {
	pst, err := ptypes.TimestampProto(sp.StartTime)
	if err != nil {
		return err
	}
	pft, err := ptypes.TimestampProto(sp.FinishTime)
	if err != nil {
		return err
	}
	var tags []*pb.Tag
	for k, v := range sp.Tags {
		vs := fmt.Sprintf("%v", v) // XXX
		tags = append(tags, &pb.Tag{
			Key:   k,
			Value: vs,
		})
	}
	for _, log := range sp.Logs {
		t, err := ptypes.TimestampProto(log.Timestamp)
		if err != nil {
			return err
		}
		ps := fmt.Sprintf("%v", log.Payload) // XXX
		tags = append(tags, &pb.Tag{
			Key:   log.Event,
			Value: ps,
			Time:  t,
		})
	}
	psp := &pb.Span{
		SpanId:        sp.SpanID,
		ParentId:      sp.ParentID,
		TraceId:       sp.TraceID,
		ServiceName:   sp.ServiceName,
		OperationName: sp.OperationName,
		StartTime:     pst,
		FinishTime:    pft,
		Flags:         sp.Flags,
		Tags:          tags,
	}
	_, err = g.client.Store(context.Background(), &pb.StoreRequest{Span: psp})
	return err
}
