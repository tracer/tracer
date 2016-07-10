package tracer

import (
	"fmt"
	"time"

	"github.com/tracer/tracer/pb"

	"github.com/golang/protobuf/ptypes"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// GRPC is a gRPC-based transport for sending spans to a server.
type GRPC struct {
	client        pb.StorerClient
	queue         []RawSpan
	ch            chan RawSpan
	flushInterval time.Duration
}

// GRPCOptions are options for the GRPC storer.
type GRPCOptions struct {
	// How many spans to queue before sending them to the server.
	// Additionally, a buffer the size of 2*QueueSize will be used to
	// process new spans. If this buffer runs full, new spans will be
	// dropped.
	QueueSize int
	// How often to flush spans, even if the queue isn't full yet.
	FlushInterval time.Duration
}

// NewGRPC returns a new Storer that sends spans via gRPC to a server.
func NewGRPC(address string, grpcOpts *GRPCOptions, opts ...grpc.DialOption) (Storer, error) {
	if grpcOpts == nil {
		grpcOpts = &GRPCOptions{1024, 1 * time.Second}
	}
	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, err
	}
	client := pb.NewStorerClient(conn)
	g := &GRPC{
		client:        client,
		queue:         make([]RawSpan, 0, grpcOpts.QueueSize),
		ch:            make(chan RawSpan, grpcOpts.QueueSize*2),
		flushInterval: grpcOpts.FlushInterval,
	}
	go g.loop()
	return g, nil
}

func (g *GRPC) loop() {
	t := time.NewTicker(g.flushInterval)
	for {
		select {
		case sp := <-g.ch:
			g.queue = append(g.queue, sp)
			if len(g.queue) == cap(g.queue) {
				g.flush()
			}
		case <-t.C:
			g.flush()
		}
	}
}

func (g *GRPC) flush() {
	var pbs []*pb.Span
	for _, sp := range g.queue {
		pst, err := ptypes.TimestampProto(sp.StartTime)
		if err != nil {
			return // XXX
		}
		pft, err := ptypes.TimestampProto(sp.FinishTime)
		if err != nil {
			return // XXX
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
				return // XXX
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
		pbs = append(pbs, psp)
	}
	if _, err := g.client.Store(context.Background(), &pb.StoreRequest{Spans: pbs}); err != nil {
		return // XXX
	}
	g.queue = g.queue[0:0]
}

// Store implements the tracer.Storer interface.
func (g *GRPC) Store(sp RawSpan) error {
	select {
	case g.ch <- sp:
	default:
	}
	return nil
}
