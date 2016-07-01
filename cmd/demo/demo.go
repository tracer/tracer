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

	var carrier opentracing.HTTPHeaderTextMapCarrier

	/* <service1> */
	span1 := tracer.StartSpan("service1")
	span2 := opentracing.StartChildSpan(span1, "rpc-req-service2")
	span2.LogEvent("cs")
	carrier = opentracing.HTTPHeaderTextMapCarrier{}
	tracer.Inject(span2, opentracing.TextMap, carrier)

	/* <service2> */
	span3, _ := tracer.Join("rpc-req-service2", opentracing.TextMap, carrier)
	span3.LogEvent("sr")
	span4 := opentracing.StartChildSpan(span3, "service2")
	span5 := opentracing.StartChildSpan(span4, "rpc-req-service3")
	span5.LogEvent("cs")
	carrier = opentracing.HTTPHeaderTextMapCarrier{}
	tracer.Inject(span5, opentracing.TextMap, carrier)

	/* <service3> */
	span6, _ := tracer.Join("rpc-req-service3", opentracing.TextMap, carrier)
	span6.LogEvent("sr")
	span7 := opentracing.StartChildSpan(span6, "service3")
	span7.Finish()
	span6.LogEvent("ss")
	span6.Finish()
	// Sen2 result to service2
	/* </service3> */
	span5.LogEvent("cr")
	span5.Finish()

	span8 := opentracing.StartChildSpan(span4, "rpc-req-service4")
	span8.LogEvent("cs")
	carrier = opentracing.HTTPHeaderTextMapCarrier{}
	tracer.Inject(span8, opentracing.TextMap, carrier)

	/* <service4> */
	span9, _ := tracer.Join("rpc-req-service4", opentracing.TextMap, carrier)
	span9.LogEvent("sr")
	span10 := opentracing.StartChildSpan(span9, "service4")
	span10.Finish()
	span9.LogEvent("ss")
	span9.Finish()
	// Send result to service2
	/* </service4> */
	span8.LogEvent("cr")
	span8.Finish()

	span4.Finish()
	span3.LogEvent("ss")
	span3.Finish()
	/* </service2> */
	span2.LogEvent("cr")
	span2.Finish()
	span1.Finish()
	/* </service1> */
}
