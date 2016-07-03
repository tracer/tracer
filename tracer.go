// Package tracer implements a Dapper-style tracing system. It is
// compatible with the Open Tracing specification.
package tracer

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
)

type Logger interface {
	Printf(format string, values ...interface{})
}

type defaultLogger struct{}

func (defaultLogger) Printf(format string, values ...interface{}) {
	log.Printf(format, values...)
}

type Joiner func(carrier interface{}) (traceID, parentID, spanID uint64, baggage map[string]string, err error)
type Injecter func(sp *Span, carrier interface{}) error

var joiners = map[interface{}]Joiner{
	opentracing.TextMap: textJoiner,
	opentracing.Binary:  binaryJoiner,
}

var injecters = map[interface{}]Injecter{
	opentracing.TextMap: textInjecter,
	opentracing.Binary:  binaryInjecter,
}

func RegisterJoiner(format interface{}, joiner Joiner) {
	joiners[format] = joiner
}

func RegisterInjecter(format interface{}, injecter Injecter) {
	injecters[format] = injecter
}

func textInjecter(sp *Span, carrier interface{}) error {
	w, ok := carrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	w.Set("Tracer-TraceId", idToHex(sp.TraceID))
	w.Set("Tracer-SpanId", idToHex(sp.SpanID))
	w.Set("Tracer-ParentSpanId", idToHex(sp.ParentID))
	for k, v := range sp.Baggage {
		w.Set("Tracer-Baggage-"+k, v)
	}
	return nil
}

func textJoiner(carrier interface{}) (traceID, parentID, spanID uint64, baggage map[string]string, err error) {
	r, ok := carrier.(opentracing.TextMapReader)
	if !ok {
		return 0, 0, 0, nil, opentracing.ErrInvalidCarrier
	}
	baggage = map[string]string{}
	err = r.ForeachKey(func(key string, val string) error {
		lower := strings.ToLower(key)
		switch lower {
		case "tracer-traceid":
			traceID = idFromHex(val)
		case "tracer-spanid":
			spanID = idFromHex(val)
		case "tracer-parentspanid":
			parentID = idFromHex(val)
		default:
			if strings.HasPrefix(lower, "tracer-baggage-") {
				key = key[len("Tracer-Baggage-"):]
				baggage[key] = val
			}
		}
		return nil
	})
	if traceID == 0 {
		return 0, 0, 0, nil, opentracing.ErrTraceNotFound
	}
	return traceID, parentID, spanID, baggage, err
}

func binaryInjecter(sp *Span, carrier interface{}) error {
	w, ok := carrier.(io.Writer)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	b := make([]byte, 8*4)
	binary.BigEndian.PutUint64(b, sp.TraceID)
	binary.BigEndian.PutUint64(b[8:], sp.SpanID)
	binary.BigEndian.PutUint64(b[16:], sp.ParentID)
	binary.BigEndian.PutUint64(b[24:], uint64(len(sp.Baggage)))
	for k, v := range sp.Baggage {
		b2 := make([]byte, 16+len(k)+len(v))
		binary.BigEndian.PutUint64(b2, uint64(len(k)))
		binary.BigEndian.PutUint64(b2[8:], uint64(len(v)))
		copy(b2[16:], k)
		copy(b2[16+len(k):], v)
		b = append(b, b2...)
	}
	_, err := w.Write(b)
	return err
}

func binaryJoiner(carrier interface{}) (traceID, parentID, spanID uint64, baggage map[string]string, err error) {
	r, ok := carrier.(io.Reader)
	if !ok {
		return 0, 0, 0, nil, opentracing.ErrInvalidCarrier
	}
	b := make([]byte, 8*4)
	if _, err := io.ReadFull(r, b); err != nil {
		if err == io.ErrUnexpectedEOF {
			return 0, 0, 0, nil, opentracing.ErrTraceNotFound
		}
		return 0, 0, 0, nil, err
	}
	traceID = binary.BigEndian.Uint64(b)
	spanID = binary.BigEndian.Uint64(b[8:])
	parentID = binary.BigEndian.Uint64(b[16:])

	n := binary.BigEndian.Uint64(b[24:])

	b = make([]byte, 8*2)
	baggage = map[string]string{}
	for i := uint64(0); i < n; i++ {
		if _, err := io.ReadFull(r, b); err != nil {
			if err == io.ErrUnexpectedEOF {
				return 0, 0, 0, nil, opentracing.ErrTraceNotFound
			}
			return 0, 0, 0, nil, err
		}

		kl := int(binary.BigEndian.Uint64(b))
		vl := int(binary.BigEndian.Uint64(b[8:]))
		if kl <= 0 || vl < 0 {
			return 0, 0, 0, nil, opentracing.ErrTraceNotFound
		}

		b2 := make([]byte, kl+vl)
		if _, err := io.ReadFull(r, b2); err != nil {
			if err == io.ErrUnexpectedEOF {
				return 0, 0, 0, nil, opentracing.ErrTraceNotFound
			}
			return 0, 0, 0, nil, err
		}
		baggage[string(b2[:kl])] = string(b2[kl:])
	}

	return traceID, parentID, spanID, baggage, nil
}

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

type RawTrace struct {
	TraceID uint64
	Spans   []RawSpan
}

// Span is an implementation of the Open Tracing Span interface.
type Span struct {
	tracer *Tracer
	RawSpan
}

type RawSpan struct {
	SpanID        uint64
	ParentID      uint64
	TraceID       uint64
	OperationName string
	StartTime     time.Time
	FinishTime    time.Time

	Tags    map[string]interface{}
	Baggage map[string]string
	Logs    []opentracing.LogData
}

func (sp *Span) SetOperationName(name string) opentracing.Span {
	sp.OperationName = name
	return sp
}

func (sp *Span) SetTag(key string, value interface{}) opentracing.Span {
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

func (sp *Span) Finish() {
	sp.FinishWithOptions(opentracing.FinishOptions{})
}

func (sp *Span) FinishWithOptions(opts opentracing.FinishOptions) {
	if opts.FinishTime.IsZero() {
		opts.FinishTime = time.Now()
	}
	sp.FinishTime = opts.FinishTime
	for _, log := range opts.BulkLogData {
		sp.Log(log)
	}
	if err := sp.tracer.storer.Store(sp.RawSpan); err != nil {
		sp.tracer.Logger.Printf("error while storing tracing span: %s", err)
	}
}

func (sp *Span) LogEvent(event string) {
	sp.Log(opentracing.LogData{
		Event: event,
	})
}

func (sp *Span) LogEventWithPayload(event string, payload interface{}) {
	sp.Log(opentracing.LogData{
		Event:   event,
		Payload: payload,
	})
}

func (sp *Span) Log(data opentracing.LogData) {
	if _, ok := valueType(data.Payload); !ok {
		sp.tracer.Logger.Printf("unsupported log payload type for event %q: %T", data.Event, data.Payload)
		return
	}
	if data.Timestamp.IsZero() {
		data.Timestamp = time.Now()
	}
	sp.Logs = append(sp.Logs, data)
}

func (sp *Span) SetBaggageItem(key, value string) opentracing.Span {
	sp.Baggage[key] = value
	return sp
}

func (sp *Span) BaggageItem(key string) string {
	return sp.Baggage[key]
}

func (sp *Span) ForeachBaggageItem(handler func(k, v string) bool) {
	for k, v := range sp.Baggage {
		if !handler(k, v) {
			return
		}
	}
}

func (sp *Span) Tracer() opentracing.Tracer {
	return sp.tracer
}

// Tracer is an implementation of the Open Tracing Tracer interface.
type Tracer struct {
	Logger Logger

	storer      Storer
	idGenerator IDGenerator
}

func NewTracer(storer Storer, idGenerator IDGenerator) *Tracer {
	return &Tracer{
		Logger:      defaultLogger{},
		storer:      storer,
		idGenerator: idGenerator,
	}
}

func (tr *Tracer) StartSpan(operationName string) opentracing.Span {
	return tr.StartSpanWithOptions(opentracing.StartSpanOptions{
		OperationName: operationName,
	})
}

func (tr *Tracer) StartSpanWithOptions(opts opentracing.StartSpanOptions) opentracing.Span {
	if opts.StartTime.IsZero() {
		opts.StartTime = time.Now()
	}
	var traceID uint64
	var parentID uint64
	if opts.Parent != nil {
		parent, ok := opts.Parent.(*Span)
		if !ok {
			panic("parent span must be of type *Span")
		}
		parentID = parent.SpanID
		traceID = parent.TraceID
	}
	id := tr.idGenerator.GenerateID()
	if traceID == 0 {
		traceID = id
	}
	sp := &Span{
		tracer: tr,
		RawSpan: RawSpan{
			OperationName: opts.OperationName,
			SpanID:        id,
			ParentID:      parentID,
			TraceID:       traceID,
			StartTime:     opts.StartTime,
		},
	}
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

func (tr *Tracer) Inject(sp opentracing.Span, format interface{}, carrier interface{}) error {
	// TODO(dh): support sampling
	span, ok := sp.(*Span)
	if !ok {
		return opentracing.ErrInvalidSpan
	}
	injecter, ok := injecters[format]
	if !ok {
		return opentracing.ErrUnsupportedFormat
	}
	return injecter(span, carrier)
}

func (tr *Tracer) Join(operationName string, format interface{}, carrier interface{}) (opentracing.Span, error) {
	// TODO(dh): support sampling
	joiner, ok := joiners[format]
	if !ok {
		return nil, opentracing.ErrUnsupportedFormat
	}
	traceID, parentID, spanID, baggage, err := joiner(carrier)
	if err != nil {
		return nil, err
	}

	return &Span{
		tracer: tr,
		RawSpan: RawSpan{
			TraceID:  traceID,
			SpanID:   spanID,
			ParentID: parentID,
			Baggage:  baggage,
		},
	}, nil
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

type Queryer interface {
	TraceWithID(id uint64) (RawTrace, error)
	SpanWithID(id uint64) (RawSpan, error)
	QueryTraces(q Query) ([]RawTrace, error)
}

type QueryTag struct {
	Key        string
	Value      string
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
