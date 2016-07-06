package main

import (
	"database/sql"
	"log"

	"honnef.co/go/tracer"
	"honnef.co/go/tracer/storage/postgres"

	_ "github.com/lib/pq"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

func main() {
	db, err := sql.Open("postgres", "user=tracer dbname=postgres password=tracer sslmode=disable")
	if err != nil {
		panic(err)
	}
	storage := postgres.New(db)
	opentracing.InitGlobalTracer(tracer.NewTracer(storage, tracer.RandomID{}))
	tracer := opentracing.GlobalTracer()

	s1 := tracer.StartSpan("frontend")
	s1.SetTag(string(ext.SpanKind), "server")
	s1.SetTag(string(ext.HTTPUrl), "/hello")
	s1.SetTag(string(ext.HTTPMethod), "GET")

	s2 := opentracing.StartChildSpan(s1, "backend.hello")
	s2.SetTag(string(ext.SpanKind), "client")
	s2.SetTag(string(ext.Component), "grpc")
	carrier := opentracing.TextMapCarrier{}
	if err := tracer.Inject(s2, opentracing.TextMap, carrier); err != nil {
		log.Println(err)
	}

	s3, err := tracer.Join("backend.hello", opentracing.TextMap, carrier)
	if err != nil {
		log.Println(err)
	}
	s3.SetTag(string(ext.SpanKind), "server")
	s3.SetTag(string(ext.Component), "grpc")

	s4 := opentracing.StartChildSpan(s3, "mysql")
	s4.SetTag(string(ext.SpanKind), "client")
	s4.SetTag(string(ext.Component), "mysql")
	s4.SetTag("sql.query", "SELECT * FROM table1")
	// The MySQL server is not instrumented, so we only get the client
	// span.
	s4.Finish()

	s5 := opentracing.StartChildSpan(s3, "redis")
	s5.SetTag(string(ext.SpanKind), "client")
	s4.SetTag(string(ext.Component), "redis")
	// The Redis server is not instrumented, so we only get the client
	// span.
	s5.Finish()

	s3.Finish()
	s2.Finish()
	s1.SetTag(string(ext.HTTPStatusCode), 200)
	s1.Finish()
}
