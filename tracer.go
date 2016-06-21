// Package tracer implements a Dapper-style tracing system. It is
// compatible with the Open Tracing specification.
package tracer

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io"
	"time"

	"github.com/opentracing/opentracing-go"
)

type Joiner func(carrier interface{}) (traceID, parentID, spanID uint64, err error)
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
	return nil
}

func textJoiner(carrier interface{}) (traceID, parentID, spanID uint64, err error) {
	r, ok := carrier.(opentracing.TextMapReader)
	if !ok {
		return 0, 0, 0, opentracing.ErrInvalidCarrier
	}
	err = r.ForeachKey(func(key string, val string) error {
		switch key {
		case "X-B3-TraceId":
			traceID = idFromHex(val)
		case "X-B3-SpanId":
			spanID = idFromHex(val)
		case "X-B3-ParentSpanId":
			parentID = idFromHex(val)
		}
		return nil
	})
	if traceID == 0 {
		return 0, 0, 0, opentracing.ErrTraceNotFound
	}
	return traceID, parentID, spanID, err
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

func binaryJoiner(carrier interface{}) (traceID, parentID, spanID uint64, err error) {
	r, ok := carrier.(io.Reader)
	if !ok {
		return 0, 0, 0, opentracing.ErrInvalidCarrier
	}
	b := make([]byte, 24)
	if _, err := io.ReadFull(r, b); err != nil {
		if err == io.ErrUnexpectedEOF {
			return 0, 0, 0, opentracing.ErrTraceNotFound
		}
		return 0, 0, 0, err
	}
	traceID = binary.BigEndian.Uint64(b)
	spanID = binary.BigEndian.Uint64(b[8:])
	parentID = binary.BigEndian.Uint64(b[16:])
	return traceID, parentID, spanID, err
}

// Span is an implementation of the Open Tracing Span interface.
type Span struct {
	tracer *Tracer

	SpanID        uint64
	ParentID      uint64
	TraceID       uint64
	OperationName string
	StartTime     time.Time
}

func (sp *Span) SetOperationName(name string) opentracing.Span {
	sp.tracer.storer.SetOperationName(sp, name)
	sp.OperationName = name
	return sp
}

func (sp *Span) SetTag(key string, value interface{}) opentracing.Span {
	sp.tracer.storer.SetTag(sp, key, value)
	return sp
}

func (sp *Span) Finish() {
	sp.FinishWithOptions(opentracing.FinishOptions{})
}

func (sp *Span) FinishWithOptions(opts opentracing.FinishOptions) {
	if opts.FinishTime.IsZero() {
		opts.FinishTime = time.Now()
	}
	sp.tracer.storer.FinishWithOptions(sp, opts)
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
	sp.tracer.storer.Log(sp, data)
}

func (sp *Span) SetBaggageItem(key, value string) opentracing.Span {
	// TODO implement
	panic("not implemented")
	return sp

}
func (sp *Span) BaggageItem(key string) string {
	// TODO implement
	panic("not implemented")
	return ""
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
	if traceID == 0 {
		traceID = tr.idGenerator.GenerateID()
	}
	id := tr.idGenerator.GenerateID()
	sp := &Span{
		tracer:        tr,
		OperationName: opts.OperationName,
		SpanID:        id,
		ParentID:      parentID,
		TraceID:       traceID,
		StartTime:     opts.StartTime,
	}
	tr.storer.AddSpan(sp)
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
	traceID, _, spanID, err := joiner(carrier)
	if err != nil {
		return nil, opentracing.ErrUnsupportedFormat
	}

	return tr.StartSpanWithOptions(opentracing.StartSpanOptions{
		OperationName: operationName,
		Parent:        &Span{TraceID: traceID, SpanID: spanID},
	}), nil
}

// IDGenerator generates IDs for traces and spans. The ID with value 0
// is reserved to mean "no parent span" and should not be generated.
type IDGenerator interface {
	GenerateID() uint64
}

// Storer maps Open Tracing operations to a backend. The backend can
// be actual storage in a database, or a transport to a remote server,
// or coalescion of spans, and so on.
//
// It is up to the implementation whether it acts on operations right
// away or if it caches them. For example, an implementation could
// wait for a call to FinishWithOptions before storing a span, at the
// risk of losing spans in case of crashes.
type Storer interface {
	// AddSpan creates a new span.
	AddSpan(sp *Span)
	// SetOperationName sets the operation name of the span.
	SetOperationName(sp *Span, name string)
	// SetTag sets the tag key to value. Duplicate keys overwrite each
	// other. Any value that can marshal to JSON is allowed.
	SetTag(sp *Span, key string, value interface{})
	// FinishWithOptions marks the span sp as done. FinishTime will
	// already be populated.
	FinishWithOptions(sp *Span, opts opentracing.FinishOptions)
	// Log logs an event. Timestamp will already be populated. The
	// payload must be a value that can marshal to JSON.
	Log(sp *Span, data opentracing.LogData)
}

type Queryer interface {
	SpanWithID(id uint64) (*Span, bool)
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
