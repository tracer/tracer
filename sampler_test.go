package tracer

import (
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

const N = int(1e6)

func TestProbabilisticSampler(t *testing.T) {
	s := NewProbabilisticSampler(1)
	n := 0
	for i := 0; i < N; i++ {
		if s.Sample(1) {
			n++
		}
	}
	if n != N {
		t.Errorf("got %d out of %d samples, expected %d", N, n, N)
	}

	s = NewProbabilisticSampler(0)
	n = 0
	for i := 0; i < N; i++ {
		if s.Sample(1) {
			n++
		}
	}
	if n != 0 {
		t.Errorf("got %d out of %d samples, expected 0", n, N)
	}

	s = NewProbabilisticSampler(0.25)
	n = 0
	for i := 0; i < N; i++ {
		if s.Sample(1) {
			n++
		}
	}
	if n > N/4+N/100 || n < N/4-N {
		t.Errorf("got %d out of %d samples, expected about %d", N, n, N/4)
	}
}

func TestConstSampler(t *testing.T) {
	s := NewConstSampler(true)
	for i := 0; i < N; i++ {
		if !s.Sample(1) {
			t.Error("expected only true samples")
		}
	}

	s = NewConstSampler(false)
	for i := 0; i < N; i++ {
		if s.Sample(1) {
			t.Error("expected only false samples")
		}
	}
}

func TestRateSampler(t *testing.T) {
	t1 := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(2 * time.Second)

	s := NewRateSampler(1000)
	s.(rateSampler).l.t = t1
	s.(rateSampler).l.nowFn = func() time.Time { return t1 }
	n := 0
	for i := 0; i < N; i++ {
		if s.Sample(1) {
			n++
		}
	}
	if n != 1000 {
		t.Errorf("got %d samples, expected %d", n, 1000)
	}

	s = NewRateSampler(1000)
	s.(rateSampler).l.t = t1
	s.(rateSampler).l.nowFn = func() time.Time { return t1 }
	n = 0
	for i := 0; i < N; i++ {
		if s.Sample(1) {
			n++
		}
		if i == N/2 {
			s.(rateSampler).l.nowFn = func() time.Time {
				return t2
			}
		}
	}
	if n != 2000 {
		t.Errorf("got %d samples, expected %d", n, 1000)
	}
}

func TestForcedSample(t *testing.T) {
	tr := &Tracer{}
	tr.Sampler = NewConstSampler(false)
	tr.idGenerator = RandomID{}
	sp := tr.StartSpan("", opentracing.Tags{string(ext.SamplingPriority): uint16(1)})
	if !sp.(*Span).Sampled() {
		t.Errorf("span wasn't sampled but expected it to be")
	}
}

func TestSamplerUse(t *testing.T) {
	tr := &Tracer{}
	tr.Sampler = NewConstSampler(true)
	tr.idGenerator = RandomID{}
	sp := tr.StartSpan("")
	if !sp.(*Span).Sampled() {
		t.Errorf("span wasn't sampled but expected it to be")
	}

	tr.Sampler = NewConstSampler(false)
	sp = tr.StartSpan("")
	if sp.(*Span).Sampled() {
		t.Errorf("span was sampled but didn't expect it to be")
	}
}
