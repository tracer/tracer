package tracer

import (
	"encoding/binary"
	"io"
	"strconv"
	"strings"

	"github.com/opentracing/opentracing-go"
)

// TODO(dh): rename Joiner to Extracter

// A Joiner extracts a SpanContext from carrier.
type Joiner func(carrier interface{}) (SpanContext, error)

// An Injecter injects a SpanContext into carrier.
type Injecter func(sm SpanContext, carrier interface{}) error

var joiners = map[interface{}]Joiner{
	opentracing.TextMap: textJoiner,
	opentracing.Binary:  binaryJoiner,
}

var injecters = map[interface{}]Injecter{
	opentracing.TextMap: textInjecter,
	opentracing.Binary:  binaryInjecter,
}

// RegisterJoiner registers a Joiner.
func RegisterJoiner(format interface{}, joiner Joiner) {
	joiners[format] = joiner
}

// RegisterInjecter registers an Injecter.
func RegisterInjecter(format interface{}, injecter Injecter) {
	injecters[format] = injecter
}

// SpanContext contains the parts of a span that will be sent to
// downstream services.
type SpanContext struct {
	TraceID  uint64            `json:"trace_id"`
	ParentID uint64            `json:"parent_id"`
	SpanID   uint64            `json:"span_id"`
	Flags    uint64            `json:"flags"`
	Baggage  map[string]string `json:"baggage"`
}

// SetBaggageItem implements the opentracing.Tracer interface.
func (c SpanContext) SetBaggageItem(key, value string) opentracing.SpanContext {
	c.Baggage[key] = value
	return c
}

// BaggageItem implements the opentracing.Tracer interface.
func (c SpanContext) BaggageItem(key string) string {
	return c.Baggage[key]
}

// ForeachBaggageItem implements the opentracing.Tracer interface.
func (c SpanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	for k, v := range c.Baggage {
		if !handler(k, v) {
			return
		}
	}
}

func textInjecter(sm SpanContext, carrier interface{}) error {
	w, ok := carrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	w.Set("Tracer-TraceId", idToHex(sm.TraceID))
	w.Set("Tracer-SpanId", idToHex(sm.SpanID))
	w.Set("Tracer-ParentSpanId", idToHex(sm.ParentID))
	w.Set("Tracer-Flags", strconv.FormatUint(sm.Flags, 10))
	for k, v := range sm.Baggage {
		w.Set("Tracer-Baggage-"+k, v)
	}
	return nil
}

func textJoiner(carrier interface{}) (SpanContext, error) {
	r, ok := carrier.(opentracing.TextMapReader)
	if !ok {
		return SpanContext{}, opentracing.ErrInvalidCarrier
	}
	ctx := SpanContext{Baggage: map[string]string{}}
	err := r.ForeachKey(func(key string, val string) error {
		lower := strings.ToLower(key)
		switch lower {
		case "tracer-traceid":
			ctx.TraceID = idFromHex(val)
		case "tracer-spanid":
			ctx.SpanID = idFromHex(val)
		case "tracer-parentspanid":
			ctx.ParentID = idFromHex(val)
		case "tracer-flags":
			ctx.Flags, _ = strconv.ParseUint(val, 10, 64)
		default:
			if strings.HasPrefix(lower, "tracer-baggage-") {
				key = key[len("Tracer-Baggage-"):]
				ctx.Baggage[key] = val
			}
		}
		return nil
	})
	if ctx.TraceID == 0 {
		return SpanContext{}, opentracing.ErrSpanContextNotFound
	}
	return ctx, err
}

func binaryInjecter(sm SpanContext, carrier interface{}) error {
	w, ok := carrier.(io.Writer)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	b := make([]byte, 8*5)
	binary.BigEndian.PutUint64(b, sm.TraceID)
	binary.BigEndian.PutUint64(b[8:], sm.SpanID)
	binary.BigEndian.PutUint64(b[16:], sm.ParentID)
	binary.BigEndian.PutUint64(b[24:], sm.Flags)
	binary.BigEndian.PutUint64(b[32:], uint64(len(sm.Baggage)))
	for k, v := range sm.Baggage {
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

func binaryJoiner(carrier interface{}) (SpanContext, error) {
	r, ok := carrier.(io.Reader)
	if !ok {
		return SpanContext{}, opentracing.ErrInvalidCarrier
	}
	ctx := SpanContext{Baggage: map[string]string{}}
	b := make([]byte, 8*5)
	if _, err := io.ReadFull(r, b); err != nil {
		if err == io.ErrUnexpectedEOF {
			return SpanContext{}, opentracing.ErrSpanContextNotFound
		}
		return SpanContext{}, err
	}
	ctx.TraceID = binary.BigEndian.Uint64(b)
	ctx.SpanID = binary.BigEndian.Uint64(b[8:])
	ctx.ParentID = binary.BigEndian.Uint64(b[16:])
	ctx.Flags = binary.BigEndian.Uint64(b[24:])
	n := binary.BigEndian.Uint64(b[32:])

	b = make([]byte, 8*2)
	for i := uint64(0); i < n; i++ {
		if _, err := io.ReadFull(r, b); err != nil {
			if err == io.ErrUnexpectedEOF {
				return SpanContext{}, opentracing.ErrSpanContextNotFound
			}
			return SpanContext{}, err
		}

		kl := int(binary.BigEndian.Uint64(b))
		vl := int(binary.BigEndian.Uint64(b[8:]))
		if kl <= 0 || vl < 0 {
			return SpanContext{}, opentracing.ErrSpanContextNotFound
		}

		b2 := make([]byte, kl+vl)
		if _, err := io.ReadFull(r, b2); err != nil {
			if err == io.ErrUnexpectedEOF {
				return SpanContext{}, opentracing.ErrSpanContextNotFound
			}
			return SpanContext{}, err
		}
		ctx.Baggage[string(b2[:kl])] = string(b2[kl:])
	}

	return ctx, nil
}
