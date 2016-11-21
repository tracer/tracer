// Command demo creates an example trace.
package main

import (
	"log"
	"time"

	"github.com/lygo/tracer"

	_ "github.com/lib/pq"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"google.golang.org/grpc"
)

func main() {
	storage, err := tracer.NewGRPC("localhost:9999", &tracer.GRPCOptions{
		QueueSize:     1024,
		FlushInterval: 1 * time.Second,
	}, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	t1 := tracer.NewTracer("frontend", storage, tracer.RandomID{})
	t2 := tracer.NewTracer("backend", storage, tracer.RandomID{})

	s1 := t1.StartSpan("frontend", ext.RPCServerOption(nil))
	ext.HTTPUrl.Set(s1, "/hello")
	ext.HTTPMethod.Set(s1, "GET")

	s2 := t1.StartSpan("backend.hello", opentracing.ChildOf(s1.Context()))
	ext.SpanKindRPCClient.Set(s2)
	ext.Component.Set(s2, "grpc")
	carrier := opentracing.TextMapCarrier{}
	if err := t1.Inject(s2.Context(), opentracing.TextMap, carrier); err != nil {
		log.Println(err)
	}

	c3, err := t2.Extract(opentracing.TextMap, carrier)
	if err != nil {
		log.Println(err)
	}
	s3 := t2.StartSpan("backend.hello", ext.RPCServerOption(c3))
	ext.Component.Set(s3, "grpc")

	s4 := t2.StartSpan("mysql", opentracing.ChildOf(s3.Context()))
	ext.SpanKindRPCClient.Set(s4)
	ext.Component.Set(s4, "mysql")
	s4.SetTag("sql.query", "SELECT * FROM table1")
	// The MySQL server is not instrumented, so we only get the client
	// span.
	s4.Finish()

	s5 := t2.StartSpan("redis", opentracing.ChildOf(s3.Context()))
	ext.SpanKindRPCClient.Set(s5)
	ext.Component.Set(s5, "redis")
	// The Redis server is not instrumented, so we only get the client
	// span.
	s5.Finish()

	s3.Finish()
	s2.Finish()

	s6 := t1.StartSpan("backend.ads", opentracing.ChildOf(s1.Context()))
	ext.SpanKindRPCClient.Set(s6)
	ext.Component.Set(s6, "grpc")
	carrier = opentracing.TextMapCarrier{}
	if err := t1.Inject(s6.Context(), opentracing.TextMap, carrier); err != nil {
		log.Println(err)
	}

	c7, err := t2.Extract(opentracing.TextMap, carrier)
	if err != nil {
		log.Println(err)
	}
	s7 := t2.StartSpan("backend.ads", ext.RPCServerOption(c7))
	ext.Component.Set(s7, "grpc")
	s7.Finish()
	s6.Finish()

	ext.HTTPStatusCode.Set(s1, 200)
	s1.Finish()

	// Wait for spans to be flushed. Production code wouldn't need
	// this.
	time.Sleep(2 * time.Second)
}
