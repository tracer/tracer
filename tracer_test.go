package tracer

import (
	"testing"

	"github.com/opentracing/opentracing-go"
)

type mockTextMapReader struct{}

var _ opentracing.TextMapReader = mockTextMapReader{}

func (mockTextMapReader) ForeachKey(fn func(string, string) error) error {
	_ = fn("X-B3-TraceId", "0000000000000457")
	_ = fn("X-B3-SpanId", "00000000000008ae")
	_ = fn("X-B3-ParentSpanId", "0000000000000d05")
	return nil
}

type mockTextMapWriter struct{}

var _ opentracing.TextMapWriter = mockTextMapWriter{}

func (mockTextMapWriter) Set(string, string) {}

func BenchmarkJoinTextMap(b *testing.B) {
	tr := &Tracer{}
	for i := 0; i < b.N; i++ {
		_, _ = tr.Join("", opentracing.TextMap, mockTextMapReader{})
	}
}

func BenchmarkInjectTextMap(b *testing.B) {
	sp := &Span{
		TraceID:  1111,
		SpanID:   2222,
		ParentID: 3333,
	}
	tr := &Tracer{}
	for i := 0; i < b.N; i++ {
		tr.Inject(sp, opentracing.TextMap, mockTextMapWriter{})
	}
}
