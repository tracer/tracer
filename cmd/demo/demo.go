package main

import (
	"database/sql"

	"honnef.co/go/tracer"
	"honnef.co/go/tracer/storage/postgres"

	_ "github.com/lib/pq"
	"github.com/opentracing/opentracing-go"
)

func main() {
	db, err := sql.Open("postgres", "user=tracer dbname=postgres password=tracer sslmode=disable")
	if err != nil {
		panic(err)
	}
	storage := postgres.New(db)
	opentracing.InitGlobalTracer(tracer.NewTracer(storage, tracer.RandomID{}))
	tracer := opentracing.GlobalTracer()

	span1 := tracer.StartSpan("frontend")
	span1.SetTag("url", "/hello")
	span1.SetTag("user_id", 123)
	span2 := opentracing.StartChildSpan(span1, "backend")
	span2.SetTag("instance", 456)
	span3 := opentracing.StartChildSpan(span2, "mysql")
	span3.LogEventWithPayload("prepare", "SELECT hello FROM world")
	span3.LogEvent("execute")
	span3.Finish()
	span4 := opentracing.StartChildSpan(span2, "redis")
	span4.LogEvent("store")
	span4.SetTag("k", "v")
	span4.SetTag("invalid", 2i)
	span4.Finish()
	span2.Finish()
	span1.Finish()
}
