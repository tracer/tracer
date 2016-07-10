// Package null is a null storage. It discards all spans and returns
// none.
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

// Null is a storage that discards all spans and doesn't return any.
type Null struct{}

// Store implements the server.Storage interface.
func (Null) Store(sp tracer.RawSpan) error { return nil }

// TraceByID implements the server.Storage interface.
func (Null) TraceByID(id uint64) (tracer.RawTrace, error) { return tracer.RawTrace{}, nil }

// SpanByID implements the server.Storage interface.
func (Null) SpanByID(id uint64) (tracer.RawSpan, error) { return tracer.RawSpan{}, nil }

// QueryTraces implements the server.Storage interface.
func (Null) QueryTraces(q server.Query) ([]tracer.RawTrace, error) { return nil, nil }

// Services implements the server.Storage interface.
func (Null) Services() ([]string, error) { return nil, nil }

// Spans implements the server.Storage interface.
func (Null) Spans(service string) ([]string, error) { return nil, nil }

// Dependencies implements the server.Storage interface.
func (Null) Dependencies() ([]server.Dependency, error) { return nil, nil }
