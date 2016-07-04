package tracer

import (
	"bytes"
	"testing"

	"github.com/opentracing/opentracing-go"
)

func TestText(t *testing.T) {
	sp := &Span{
		RawSpan: RawSpan{
			SpanID:   1,
			ParentID: 2,
			TraceID:  3,
			Flags:    FlagSampled,
			Baggage: map[string]string{
				"k1": "v1",
				"k2": "",
			},
		},
	}

	carrier := opentracing.TextMapCarrier{}
	if err := textInjecter(sp, carrier); err != nil {
		t.Fatal("unexpected error: ", err)
	}
	context, err := textJoiner(carrier)
	if err != nil {
		t.Fatal("unexpected error: ", err)
	}
	if context.TraceID != sp.TraceID || context.ParentID != sp.ParentID || context.SpanID != sp.SpanID || context.Flags != sp.Flags ||
		len(context.Baggage) != 2 || context.Baggage["k1"] != "v1" || context.Baggage["k2"] != "" {

		t.Errorf("got (%d, %d, %d, %d, %v), want (%d, %d, %d, %d, %v)",
			context.TraceID, context.ParentID, context.SpanID, context.Flags, context.Baggage,
			sp.TraceID, sp.ParentID, sp.SpanID, sp.Flags, sp.Baggage)
	}
}

func TestBinary(t *testing.T) {
	sp := &Span{
		RawSpan: RawSpan{
			SpanID:   1,
			ParentID: 2,
			TraceID:  3,
			Flags:    FlagSampled,
			Baggage: map[string]string{
				"k1": "v1",
				"k2": "",
			},
		},
	}
	buf := &bytes.Buffer{}
	if err := binaryInjecter(sp, buf); err != nil {
		t.Fatal("unexpected error: ", err)
	}
	context, err := binaryJoiner(buf)
	if err != nil {
		t.Fatal("unexpected error: ", err)
	}
	if context.TraceID != sp.TraceID || context.ParentID != sp.ParentID || context.SpanID != sp.SpanID || context.Flags != sp.Flags ||
		len(context.Baggage) != 2 || context.Baggage["k1"] != "v1" || context.Baggage["k2"] != "" {

		t.Errorf("got (%d, %d, %d, %d, %v), want (%d, %d, %d, %d, %v)",
			context.TraceID, context.ParentID, context.SpanID, context.Flags, context.Baggage,
			sp.TraceID, sp.ParentID, sp.SpanID, sp.Flags, sp.Baggage)
	}
}
