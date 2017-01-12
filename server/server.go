// Package server implements the Tracer server.
package server

import (
	"strings"
	"time"

	"github.com/tracer/tracer"
)

// A StorageTransportEngine returns an instance of a storage transport.
type StorageTransportEngine func(srv *Server, conf map[string]interface{}) (StorageTransport, error)

// A QueryTransportEngine returns an instance of a query transport.
type QueryTransportEngine func(srv *Server, conf map[string]interface{}) (QueryTransport, error)

// A StorageEngine returns an instance of a storage.
type StorageEngine func(conf map[string]interface{}) (Storage, error)

var storageTransportEngines = map[string]StorageTransportEngine{}
var queryTransportEngines = map[string]QueryTransportEngine{}
var storageEngines = map[string]StorageEngine{}

// RegisterStorageTransport registers a storage transport.
func RegisterStorageTransport(name string, engine StorageTransportEngine) {
	storageTransportEngines[name] = engine
}

// GetStorageTransport returns a storage transport by name.
func GetStorageTransport(name string) (StorageTransportEngine, bool) {
	transport, ok := storageTransportEngines[name]
	return transport, ok
}

// RegisterQueryTransport registers a query transport.
func RegisterQueryTransport(name string, engine QueryTransportEngine) {
	queryTransportEngines[name] = engine
}

// GetQueryTransport returns a query transport by name.
func GetQueryTransport(name string) (QueryTransportEngine, bool) {
	transport, ok := queryTransportEngines[name]
	return transport, ok
}

// RegisterStorage registers a storage engine.
func RegisterStorage(name string, engine StorageEngine) {
	storageEngines[name] = engine
}

// GetStorage returns a storage engine by name.
func GetStorage(name string) (StorageEngine, bool) {
	storer, ok := storageEngines[name]
	return storer, ok
}

// A StorageTransport accepts spans via some protocol and sends them
// to a Storer.
type StorageTransport interface {
	// Start starts the transport.
	Start() error
}

// QueryTransport accepts requests via some protocol and answers them.
type QueryTransport interface {
	// Start starts the transport.
	Start() error
}

// Storage allows storing and querying spans.
type Storage interface {
	tracer.Storer
	Queryer
}

// A Purger can delete all traces starting before a certain date.
type Purger interface {
	Purge(before time.Time) error
}

// A Queryer is a backend that allows fetching traces and spans by ID
// or via a more advanced query.
type Queryer interface {
	// TraceByID returns a trace with a specific ID.
	TraceByID(id uint64) (tracer.RawTrace, error)
	// SpanByID returns a span with a specific ID.
	SpanByID(id uint64) (tracer.RawSpan, error)
	// QueryTraces returns all traces that match a query.
	QueryTraces(q Query) ([]tracer.RawTrace, error)

	// Services returns a list of all services.
	Services() ([]string, error)
	// Operations returns a list of all operations.
	Operations(service string) ([]string, error)
	// Dependencies returns the dependencies between services.
	Dependencies() ([]Dependency, error)
}

// Dependency describes the dependency of one service on another.
type Dependency struct {
	Parent string
	Child  string
	Count  uint64
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

// A Query describes the various conditionals of a query for a trace.
//
// All conditions are ANDed together. Zero values are understood as
// the lack of a constraint.
type Query struct {
	// Only return traces that started at or after this time.
	StartTime time.Time
	// Only return traces that finished before or at this time.
	FinishTime time.Time
	// Only return traces where a span has this operation name.
	OperationName string
	// Only return traces that lasted at least this long.
	MinDuration time.Duration
	// Only return traces that lasted at most this long.
	MaxDuration time.Duration
	// Only return traces where all spans combined have all of these
	// tags.
	AndTags []QueryTag
	// Only return traces where all spans combined have at least one
	// of these tags.
	OrTags []QueryTag
	// How many traces to return. Zero means no limit.
	Num int
	// ServiceNames to filter by.
	ServiceNames []string
}

// Server is an instance of the Tracer application.
type Server struct {
	Storage          Storage
	StorageTransport StorageTransport
	QueryTransports  []QueryTransport
}

type errors struct {
	errs []error
}

func (errs errors) Error() string {
	var s []string
	for _, err := range errs.errs {
		s = append(s, err.Error())
	}
	return strings.Join(s, "\n")
}

func (srv *Server) Start() error {
	errs := make(chan error)
	go func() {
		errs <- srv.StorageTransport.Start()
	}()
	for _, t := range srv.QueryTransports {
		t := t
		go func() {
			errs <- t.Start()
		}()
	}
	var out errors
	for i := 0; i < len(srv.QueryTransports)+1; i++ {
		if err := <-errs; err != nil {
			out.errs = append(out.errs, err)
		}
	}
	if len(out.errs) == 0 {
		return nil
	}
	return out
}
