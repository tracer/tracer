package server

import (
	"time"

	"github.com/tracer/tracer"
)

type StorageTransportEngine func(srv *Server, conf map[string]interface{}) (StorageTransport, error)

var engines = map[string]StorageTransportEngine{}

func RegisterStorageTransport(name string, engine StorageTransportEngine) {
	engines[name] = engine
}

func GetStorageTransport(name string) (StorageTransportEngine, bool) {
	transport, ok := engines[name]
	return transport, ok
}

type StorageTransport interface {
	Start() error
}

type Storage interface {
	tracer.Storer
	Queryer
}

type Server struct {
	Storage Storage
}

// A Queryer is a backend that allows fetching traces and spans by ID
// or via a more advanced query.
type Queryer interface {
	// TraceWithID returns a trace with a specific ID.
	TraceWithID(id uint64) (tracer.RawTrace, error)
	// SpanWithID returns a span with a specific ID.
	SpanWithID(id uint64) (tracer.RawSpan, error)
	// QueryTraces returns all traces that match a query.
	QueryTraces(q Query) ([]tracer.RawTrace, error)
}

// QueryTag describes a single tag or log entry that should be queried
// for.
type QueryTag struct {
	// The key of the tag.
	Key string
	// The value of the tag.
	Value string
	// Whether the value should be checked for.
	CheckValue bool
}

type Query struct {
	StartTime     time.Time
	FinishTime    time.Time
	OperationName string
	MinDuration   time.Duration
	MaxDuration   time.Duration
	AndTags       []QueryTag
	OrTags        []QueryTag
}
