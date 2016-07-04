package tracer

import (
	"encoding/binary"
	"io"
	"strconv"
	"strings"

	"github.com/opentracing/opentracing-go"
)

type Joiner func(carrier interface{}) (Context, error)
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

type Context struct {
	TraceID  uint64
	ParentID uint64
	SpanID   uint64
	Flags    uint64
	Baggage  map[string]string
}

func textInjecter(sp *Span, carrier interface{}) error {
	w, ok := carrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	w.Set("Tracer-TraceId", idToHex(sp.TraceID))
	w.Set("Tracer-SpanId", idToHex(sp.SpanID))
	w.Set("Tracer-ParentSpanId", idToHex(sp.ParentID))
	w.Set("Tracer-Flags", strconv.FormatUint(sp.Flags, 10))
	for k, v := range sp.Baggage {
		w.Set("Tracer-Baggage-"+k, v)
	}
	return nil
}

func textJoiner(carrier interface{}) (Context, error) {
	r, ok := carrier.(opentracing.TextMapReader)
	if !ok {
		return Context{}, opentracing.ErrInvalidCarrier
	}
	ctx := Context{Baggage: map[string]string{}}
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
		return Context{}, opentracing.ErrTraceNotFound
	}
	return ctx, err
}

func binaryInjecter(sp *Span, carrier interface{}) error {
	w, ok := carrier.(io.Writer)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	b := make([]byte, 8*5)
	binary.BigEndian.PutUint64(b, sp.TraceID)
	binary.BigEndian.PutUint64(b[8:], sp.SpanID)
	binary.BigEndian.PutUint64(b[16:], sp.ParentID)
	binary.BigEndian.PutUint64(b[24:], sp.Flags)
	binary.BigEndian.PutUint64(b[32:], uint64(len(sp.Baggage)))
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

func binaryJoiner(carrier interface{}) (Context, error) {
	r, ok := carrier.(io.Reader)
	if !ok {
		return Context{}, opentracing.ErrInvalidCarrier
	}
	ctx := Context{Baggage: map[string]string{}}
	b := make([]byte, 8*5)
	if _, err := io.ReadFull(r, b); err != nil {
		if err == io.ErrUnexpectedEOF {
			return Context{}, opentracing.ErrTraceNotFound
		}
		return Context{}, err
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
				return Context{}, opentracing.ErrTraceNotFound
			}
			return Context{}, err
		}

		kl := int(binary.BigEndian.Uint64(b))
		vl := int(binary.BigEndian.Uint64(b[8:]))
		if kl <= 0 || vl < 0 {
			return Context{}, opentracing.ErrTraceNotFound
		}

		b2 := make([]byte, kl+vl)
		if _, err := io.ReadFull(r, b2); err != nil {
			if err == io.ErrUnexpectedEOF {
				return Context{}, opentracing.ErrTraceNotFound
			}
			return Context{}, err
		}
		ctx.Baggage[string(b2[:kl])] = string(b2[kl:])
	}

	return ctx, nil
}
