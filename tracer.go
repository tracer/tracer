package tracer

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"time"

	"github.com/opentracing/opentracing-go"
)

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
	return sp

}
func (sp *Span) BaggageItem(key string) string {
	// TODO implement
	return ""
}

func (sp *Span) Tracer() opentracing.Tracer {
	return sp.tracer
}

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
	switch format {
	case opentracing.TextMap:
		w := carrier.(opentracing.TextMapWriter)
		w.Set("X-B3-TraceId", idToHex(sp.(*Span).TraceID))
		w.Set("X-B3-SpanId", idToHex(sp.(*Span).SpanID))
		w.Set("X-B3-ParentSpanId", idToHex(sp.(*Span).ParentID))
		// TODO(dh): support sampling
	case opentracing.Binary:
		// TODO implement
	default:
		return opentracing.ErrUnsupportedFormat
	}
	return nil
}

func (tr *Tracer) Join(operationName string, format interface{}, carrier interface{}) (opentracing.Span, error) {
	switch format {
	case opentracing.TextMap:
		w := carrier.(opentracing.TextMapReader)
		sp := &Span{tracer: tr}
		err := w.ForeachKey(func(key string, val string) error {
			switch key {
			case "X-B3-TraceId":
				sp.TraceID = idFromHex(val)
			case "X-B3-SpanId":
				sp.SpanID = idFromHex(val)
			case "X-B3-ParentSpanId":
				sp.ParentID = idFromHex(val)
			}
			return nil
		})
		return sp, err
	case opentracing.Binary:
		// TODO implement
	default:
		return nil, opentracing.ErrUnsupportedFormat
	}
	return nil, nil
}

type IDGenerator interface {
	GenerateID() uint64
}

type Storer interface {
	AddSpan(sp *Span)
	SetOperationName(sp *Span, name string)
	SetTag(sp *Span, key string, value interface{})
	FinishWithOptions(sp *Span, opts opentracing.FinishOptions)
	Log(sp *Span, data opentracing.LogData)
}

type Queryer interface {
	SpanWithID(id uint64) (*Span, bool)
}

// RandomID generates random IDs by using crypto/rand.
type RandomID struct{}

func (RandomID) GenerateID() uint64 {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return binary.BigEndian.Uint64(b)
}
