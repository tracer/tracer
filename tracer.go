// Package tracer implements a Dapper-style tracing system. It is
// compatible with the OpenTracing specification.
//
// Sampling
//
// To keep the overhead incurred by tracing low, only a subset of
// requests should be traced. This is achieved by a sampler. Each
// tracer has a sampler that may sample requests based on chance, a
// rate, or possibly other mechanisms. The default sampler samples all
// requests, which is useful for testing, and viable for low-traffic
// systems.
//
// Only root spans make sampling decisions. Child spans will inherit
// the sampling decisions of the root spans.
//
// Errors and logging
//
// The instrumentation is defensive and will never purposefully panic.
// At the same time, most functions do not return errors, because
// they'll be called by automatic instrumentation, hidden from the
// user. Instead, errors will be logged.
package tracer

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
)

// The various flags of a Span.
const (
	// The Span has been sampled.
	FlagSampled = 1 << iota
)

// A Logger logs messages.
type Logger interface {
	// Printf logs a single message, given a format and values. The
	// format is documented in the fmt package.
	Printf(format string, values ...interface{})
}

type defaultLogger struct{}

func (defaultLogger) Printf(format string, values ...interface{}) {
	log.Printf(format, values...)
}

// valueType returns the broad categorization of a value's type and
// whether it is permitted as a payload.
func valueType(v interface{}) (string, bool) {
	if v == nil {
		return "", true
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return "", false
	}
	switch rv.Type().Kind() {
	case reflect.Bool:
		return "boolean", true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return "number", true
	case reflect.String:
		return "string", true
	}
	return "", false
}

// A RawTrace contains all the data associated with a trace.
type RawTrace struct {
	TraceID   uint64        `json:"trace_id"`
	Spans     []RawSpan     `json:"spans"`
	Relations []RawRelation `json:"relations"`
}

// A RawRelation represents the relation between two spans.
type RawRelation struct {
	ParentID uint64 `json:"parent_id"`
	ChildID  uint64 `json:"child_id"`
	Kind     string `json:"kind"`
}

// Span is an implementation of the OpenTracing Span interface.
type Span struct {
	mu     sync.RWMutex
	tracer *Tracer
	RawSpan
}

// A RawSpan contains all the data associated with a span.
type RawSpan struct {
	SpanContext
	ServiceName   string    `json:"service_name"`
	OperationName string    `json:"operation_name"`
	StartTime     time.Time `json:"start_time"`
	FinishTime    time.Time `json:"finish_time"`

	Tags map[string]interface{} `json:"tags"`
	Logs []opentracing.LogData  `json:"logs"`
}

// Sampled reports whether this span was sampled.
func (sp *Span) Sampled() bool {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.sampled()
}

func (sp *Span) sampled() bool {
	return (sp.Flags & FlagSampled) > 0
}

// SetOperationName implements the opentracing.Span interface.
func (sp *Span) SetOperationName(name string) opentracing.Span {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.OperationName = name
	return sp
}

// SetTag implements the opentracing.Span interface.
func (sp *Span) SetTag(key string, value interface{}) opentracing.Span {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if !sp.sampled() {
		return sp
	}
	if _, ok := valueType(value); !ok {
		sp.tracer.Logger.Printf("unsupported tag value type for tag %q: %T", key, value)
		return sp
	}
	if sp.Tags == nil {
		sp.Tags = map[string]interface{}{}
	}
	sp.Tags[key] = value
	return sp
}

// Finish implements the opentracing.Span interface.
func (sp *Span) Finish() {
	if !sp.Sampled() {
		return
	}
	sp.FinishWithOptions(opentracing.FinishOptions{})
}

// FinishWithOptions implements the opentracing.Span interface.
func (sp *Span) FinishWithOptions(opts opentracing.FinishOptions) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if !sp.sampled() {
		return
	}
	if opts.FinishTime.IsZero() {
		opts.FinishTime = time.Now()
	}
	sp.FinishTime = opts.FinishTime
	for _, log := range opts.BulkLogData {
		sp.log(log)
	}
	if err := sp.tracer.storer.Store(sp.RawSpan); err != nil {
		sp.tracer.Logger.Printf("error while storing tracing span: %s", err)
	}
}

// LogEvent implements the opentracing.Span interface.
func (sp *Span) LogEvent(event string) {
	if !sp.Sampled() {
		return
	}
	sp.Log(opentracing.LogData{
		Event: event,
	})
}

// LogEventWithPayload implements the opentracing.Span interface.
func (sp *Span) LogEventWithPayload(event string, payload interface{}) {
	if !sp.Sampled() {
		return
	}
	sp.Log(opentracing.LogData{
		Event:   event,
		Payload: payload,
	})
}

// Log implements the opentracing.Span interface.
func (sp *Span) Log(data opentracing.LogData) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.log(data)
}

func (sp *Span) log(data opentracing.LogData) {
	if !sp.sampled() {
		return
	}
	if _, ok := valueType(data.Payload); !ok {
		sp.tracer.Logger.Printf("unsupported log payload type for event %q: %T", data.Event, data.Payload)
		return
	}
	if data.Timestamp.IsZero() {
		data.Timestamp = time.Now()
	}
	sp.Logs = append(sp.Logs, data)
}

// Context implements the opentracing.Span interface.
func (sp *Span) Context() opentracing.SpanContext {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.SpanContext
}

// Tracer implements the opentracing.Span interface.
func (sp *Span) Tracer() opentracing.Tracer {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.tracer
}

// Tracer is an implementation of the OpenTracing Tracer interface.
type Tracer struct {
	ServiceName string
	Logger      Logger
	Sampler     Sampler

	storer      Storer
	idGenerator IDGenerator
}

// NewTracer returns a new tracer.
func NewTracer(serviceName string, storer Storer, idGenerator IDGenerator) *Tracer {
	return &Tracer{
		ServiceName: serviceName,
		Logger:      defaultLogger{},
		Sampler:     NewConstSampler(true),
		storer:      storer,
		idGenerator: idGenerator,
	}
}

// StartSpan implements the opentracing.Tracer interface.
func (tr *Tracer) StartSpan(operationName string, opts ...opentracing.StartSpanOption) opentracing.Span {
	var sopts opentracing.StartSpanOptions
	for _, opt := range opts {
		opt.Apply(&sopts)
	}
	if sopts.StartTime.IsZero() {
		sopts.StartTime = time.Now()
	}

	id := tr.idGenerator.GenerateID()
	sp := &Span{
		tracer: tr,
		RawSpan: RawSpan{
			SpanContext: SpanContext{
				SpanID:  id,
				TraceID: id,
			},
			ServiceName:   tr.ServiceName,
			OperationName: operationName,
			StartTime:     sopts.StartTime,
		},
	}
	if len(sopts.References) > 0 {
		// TODO(dh): support multiple parents, support ChildOf and
		// FollowsFrom as separate kinds of relations.
		parent, ok := sopts.References[0].Referee.(SpanContext)
		if !ok {
			panic("parent span must be of type *Span")
		}
		sp.ParentID = parent.SpanID
		sp.TraceID = parent.TraceID
		sp.Flags = parent.Flags
	} else {
		if tr.Sampler.Sample(id) {
			sp.Flags |= FlagSampled
		}
	}
	sp.Tags = sopts.Tags
	return sp
}

func idToHex(id uint64) string {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)
	return hex.EncodeToString(b)
}

func idFromHex(s string) uint64 {
	b, _ := hex.DecodeString(s)
	return binary.BigEndian.Uint64(b)
}

// Inject implements the opentracing.Tracer interface.
func (tr *Tracer) Inject(sm opentracing.SpanContext, format interface{}, carrier interface{}) error {
	context, ok := sm.(SpanContext)
	if !ok {
		return opentracing.ErrInvalidSpanContext
	}
	injecter, ok := injecters[format]
	if !ok {
		return opentracing.ErrUnsupportedFormat
	}
	return injecter(context, carrier)
}

// Extract implements the opentracing.Tracer interface.
func (tr *Tracer) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	extracter, ok := extracters[format]
	if !ok {
		return nil, opentracing.ErrUnsupportedFormat
	}
	context, err := extracter(carrier)
	return context, err
}

// IDGenerator generates IDs for traces and spans. The ID with value 0
// is reserved to mean "no parent span" and should not be generated.
type IDGenerator interface {
	GenerateID() uint64
}

// A Storer stores a finished span. "Storing" a span may either mean
// saving it in a storage engine, or sending it to a remote
// collector.
//
// If a span with the same ID and the same trace ID already exists,
// the existing and new spans should be merged into one span.
//
// Because spans are only stored once they're done, children will be
// stored before their parents.
type Storer interface {
	Store(sp RawSpan) error
}

var _ IDGenerator = RandomID{}

// RandomID generates random IDs by using crypto/rand.
type RandomID struct{}

// GenerateID generates an ID.
func (RandomID) GenerateID() uint64 {
	b := make([]byte, 8)
	for {
		_, _ = rand.Read(b)
		x := binary.BigEndian.Uint64(b)
		if x != 0 {
			return x
		}
	}
}
