package main

import (
	"honnef.co/go/tracer"
	"honnef.co/go/tracer/storage/bolt"

	"github.com/opentracing/opentracing-go"
)

func main() {
	storage, err := bolt.New("/tmp/db")
	if err != nil {
		panic(err)
	}
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
	span4.Finish()
	span2.Finish()
	span1.Finish()
}
