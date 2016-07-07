package main

import (
	"log"

	"github.com/tracer/tracer"

	_ "github.com/lib/pq"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"google.golang.org/grpc"
)

func main() {
	storage, err := tracer.NewGRPC("localhost:9999", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	t1 := tracer.NewTracer("frontend", storage, tracer.RandomID{})
	t2 := tracer.NewTracer("backend", storage, tracer.RandomID{})

	s1 := t1.StartSpan("frontend", ext.RPCServerOption(nil))
	s1.SetTag(string(ext.HTTPUrl), "/hello")
	s1.SetTag(string(ext.HTTPMethod), "GET")

	s2 := t1.StartSpan("backend.hello", opentracing.ChildOf(s1.Context()))
	s2.SetTag(string(ext.SpanKind), ext.SpanKindRPCClient)
	s2.SetTag(string(ext.Component), "grpc")
	carrier := opentracing.TextMapCarrier{}
	if err := t1.Inject(s2.Context(), opentracing.TextMap, carrier); err != nil {
		log.Println(err)
	}

	c3, err := t2.Extract(opentracing.TextMap, carrier)
	if err != nil {
		log.Println(err)
	}
	s3 := t2.StartSpan("backend.hello", ext.RPCServerOption(c3))
	s3.SetTag(string(ext.Component), "grpc")

	s4 := t2.StartSpan("mysql", opentracing.ChildOf(s3.Context()))
	s4.SetTag(string(ext.SpanKind), ext.SpanKindRPCClient)
	s4.SetTag(string(ext.Component), "mysql")
	s4.SetTag("sql.query", "SELECT * FROM table1")
	// The MySQL server is not instrumented, so we only get the client
	// span.
	s4.Finish()

	s5 := t2.StartSpan("redis", opentracing.ChildOf(s3.Context()))
	s5.SetTag(string(ext.SpanKind), ext.SpanKindRPCClient)
	s4.SetTag(string(ext.Component), "redis")
	// The Redis server is not instrumented, so we only get the client
	// span.
	s5.Finish()

	s3.Finish()
	s2.Finish()
	s1.SetTag(string(ext.HTTPStatusCode), 200)
	s1.Finish()
}
