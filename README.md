# Tracer – Zipping through time

Tracer is a distributed tracing system, designed after
[Dapper](http://research.google.com/pubs/pub36356.html). The
instrumentation is compatible with the
[OpenTracing specification](http://opentracing.io/).

## Status

Tracer is currently in alpha state. It is in a working state, but very
much unfinished, hardly tested and will contain bugs. You're welcome
to test it, though!

## Quickstart

The following steps will install Tracer, the Tracer UI and set up
PostgreSQL to be used as a storage backend.

The following software is required:

- Go 1.6 or later for building Tracer
- PostgreSQL 9.5 or later for the storage engine

### Installation

```
go get github.com/tracer/tracer/cmd/tracer
go get github.com/tracer/tracer-ui/cmd/tracer-ui
```

### Configuration

Create a PostgreSQL user and schema for Tracer and import the file
`$GOPATH/src/github.com/tracer/tracer/storage/postgresql/schema.sql`.
The example configuration uses the username and password `tracer` and
the database `postgres`, but you're free to edit the config.

Now you can start Tracer and its UI:

```
cp $GOPATH/src/github.com/tracer/tracer/cmd/tracer/example.conf .
# possibly edit example.conf
$GOPATH/bin/tracer -c example.conf &
$GOPATH/bin/tracer-ui -t $GOPATH/src/github.com/tracer/tracer-ui/zipkin-ui &
```

To insert a basic demo trace, run

```
go run $GOPATH/src/github.com/tracer/tracer/cmd/demo/demo.go
```

To view the UI, point your browser at http://localhost:9997/.

If you want to add instrumentation to your own code, check out
[OpenTracing](http://opentracing.io/) and
[opentracing-go](https://godoc.org/github.com/opentracing/opentracing-go)
for the API. To instantiate a Tracer instance that logs to the server
you just started, write something like this:

```
import "github.com/tracer/tracer"

...

storage, err := tracer.NewGRPC("localhost:9999", &tracer.GRPCOptions{
	QueueSize:     1024,
   	FlushInterval: 1 * time.Second,
}, grpc.WithInsecure())
if err != nil {
	log.Fatal(err)
}
t := tracer.NewTracer("frontend", storage, tracer.RandomID{})
```

This will create a tracer `t` that sends traces via gRPC to your server.

For more information on Tracer's instrumentation API check
[godoc.org](https://godoc.org/github.com/tracer/tracer).
