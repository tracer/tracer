// Package tracer implements a Dapper-style tracing system. It is
// compatible with the Open Tracing specification.
package tracer

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io"
	"log"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
)

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
	w.Set("X-B3-TraceId", idToHex(sp.TraceID))
	w.Set("X-B3-SpanId", idToHex(sp.SpanID))
	w.Set("X-B3-ParentSpanId", idToHex(sp.ParentID))
	for k, v := range sp.Baggage {
		w.Set("X-B3-Baggage-"+k, v)
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
		case "x-b3-traceid":
			traceID = idFromHex(val)
		case "x-b3-spanid":
			spanID = idFromHex(val)
		case "x-b3-parentspanid":
			parentID = idFromHex(val)
		default:
			if strings.HasPrefix(lower, "x-b3-baggage-") {
				key = key[len("X-B3-Baggage-"):]
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
	b := make([]byte, 24)
	binary.BigEndian.PutUint64(b, sp.TraceID)
	binary.BigEndian.PutUint64(b[8:], sp.TraceID)
	binary.BigEndian.PutUint64(b[16:], sp.TraceID)
	_, err := w.Write(b)
	return err
}

func binaryJoiner(carrier interface{}) (traceID, parentID, spanID uint64, baggage map[string]string, err error) {
	r, ok := carrier.(io.Reader)
	if !ok {
		return 0, 0, 0, nil, opentracing.ErrInvalidCarrier
	}
	b := make([]byte, 24)
	if _, err := io.ReadFull(r, b); err != nil {
		if err == io.ErrUnexpectedEOF {
			return 0, 0, 0, nil, opentracing.ErrTraceNotFound
		}
		return 0, 0, 0, nil, err
	}
	traceID = binary.BigEndian.Uint64(b)
	spanID = binary.BigEndian.Uint64(b[8:])
	parentID = binary.BigEndian.Uint64(b[16:])
	return traceID, parentID, spanID, nil, err
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
		log.Println("error while storing tracing span:", err)
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
	storer      Storer
	idGenerator IDGenerator
}

func NewTracer(storer Storer, idGenerator IDGenerator) *Tracer {
	return &Tracer{storer, idGenerator}
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
	QueryTraces(q Query) ([]RawSpan, error)
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
	AndTags       []QueryTag
	OrTags        []QueryTag
}

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
