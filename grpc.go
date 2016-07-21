package tracer

import (
	"fmt"
	"time"

	"github.com/tracer/tracer/pb"

	"github.com/golang/protobuf/ptypes"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// GRPC is a gRPC-based transport for sending spans to a server.
type GRPC struct {
	client        pb.StorerClient
	queue         []RawSpan
	ch            chan RawSpan
	flushInterval time.Duration
	logger        Logger

	stored  prometheus.Counter
	dropped prometheus.Counter
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
	// Where to log errors. If nil, the default logger will be used.
	Logger Logger
}

// NewGRPC returns a new Storer that sends spans via gRPC to a server.
func NewGRPC(address string, grpcOpts *GRPCOptions, opts ...grpc.DialOption) (Storer, error) {
	if grpcOpts == nil {
		grpcOpts = &GRPCOptions{
			QueueSize:     1024,
			FlushInterval: 1 * time.Second,
		}
	}
	if grpcOpts.Logger == nil {
		grpcOpts.Logger = defaultLogger{}
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
		logger:        grpcOpts.Logger,

		stored: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tracer_stored_spans_total",
			Help: "Number of stored spans",
		}),
		dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tracer_dropped_spans_total",
			Help: "Number of dropped spans",
		}),
	}
	err = prometheus.Register(g.dropped)
	if err != nil {
		g.logger.Printf("couldn't register prometheus counter: %s", err)
	}
	err = prometheus.Register(g.stored)
	if err != nil {
		g.logger.Printf("couldn't register prometheus counter: %s", err)
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
				if err := g.flush(); err != nil {
					g.logger.Printf("couldn't flush spans: %s", err)
				}
			}
		case <-t.C:
			if err := g.flush(); err != nil {
				g.logger.Printf("couldn't flush spans: %s", err)
			}
		}
	}
}

func (g *GRPC) flush() error {
	var pbs []*pb.Span
	for _, sp := range g.queue {
		pst, err := ptypes.TimestampProto(sp.StartTime)
		if err != nil {
			g.logger.Printf("dropping span because of error: %s", err)
			continue
		}
		pft, err := ptypes.TimestampProto(sp.FinishTime)
		if err != nil {
			g.logger.Printf("dropping span because of error: %s", err)
			continue
		}
		var tags []*pb.Tag
		for k, v := range sp.Tags {
			vs := fmt.Sprintf("%v", v) // XXX
			tags = append(tags, &pb.Tag{
				Key:   k,
				Value: vs,
			})
		}
		for _, l := range sp.Logs {
			t, err := ptypes.TimestampProto(l.Timestamp)
			if err != nil {
				g.logger.Printf("dropping log entry because of error: %s", err)
				continue
			}
			ps := fmt.Sprintf("%v", l.Payload) // XXX
			tags = append(tags, &pb.Tag{
				Key:   l.Event,
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
	g.queue = g.queue[0:0]
	if _, err := g.client.Store(context.Background(), &pb.StoreRequest{Spans: pbs}); err != nil {
		return err
	}
	return nil
}

// Store implements the tracer.Storer interface.
func (g *GRPC) Store(sp RawSpan) error {
	select {
	case g.ch <- sp:
		g.stored.Inc()
	default:
		g.dropped.Inc()
	}
	return nil
}
