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
	traceID, parentID, spanID, flags, baggage, err := textJoiner(carrier)
	if err != nil {
		t.Fatal("unexpected error: ", err)
	}
	if traceID != sp.TraceID || parentID != sp.ParentID || spanID != sp.SpanID || flags != sp.Flags ||
		len(baggage) != 2 || baggage["k1"] != "v1" || baggage["k2"] != "" {

		t.Errorf("got (%d, %d, %d, %d, %v), want (%d, %d, %d, %d, %v)",
			traceID, parentID, spanID, flags, baggage,
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
	traceID, parentID, spanID, flags, baggage, err := binaryJoiner(buf)
	if err != nil {
		t.Fatal("unexpected error: ", err)
	}
	if traceID != sp.TraceID || parentID != sp.ParentID || spanID != sp.SpanID || flags != sp.Flags ||
		len(baggage) != 2 || baggage["k1"] != "v1" || baggage["k2"] != "" {

		t.Errorf("got (%d, %d, %d, %d, %v), want (%d, %d, %d, %d, %v)",
			traceID, parentID, spanID, flags, baggage,
			sp.TraceID, sp.ParentID, sp.SpanID, sp.Flags, sp.Baggage)
	}
}
