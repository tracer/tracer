package null

import (
	"github.com/tracer/tracer"
	"github.com/tracer/tracer/server"
)

func init() {
	server.RegisterStorage("null", setup)
}

func setup(map[string]interface{}) (server.Storage, error) {
	return Null{}, nil
}

var _ server.Storage = Null{}

type Null struct{}

func (Null) Store(sp tracer.RawSpan) error                         { return nil }
func (Null) TraceByID(id uint64) (tracer.RawTrace, error)          { return tracer.RawTrace{}, nil }
func (Null) SpanByID(id uint64) (tracer.RawSpan, error)            { return tracer.RawSpan{}, nil }
func (Null) QueryTraces(q server.Query) ([]tracer.RawTrace, error) { return nil, nil }
func (Null) Services() ([]string, error)                           { return nil, nil }
func (Null) Spans(service string) ([]string, error)                { return nil, nil }
func (Null) Dependencies() ([]server.Dependency, error)            { return nil, nil }
