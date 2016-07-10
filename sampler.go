package tracer

import (
	"math/rand"
	"sync"
	"time"
)

// A Sampler determines whether a span should be sampled or not by
// returning true or false.
type Sampler interface {
	Sample(id uint64) bool
}

type constSampler struct {
	decision bool
}

// NewConstSampler returns a constant sampler that always returns the
// same decision.
func NewConstSampler(decision bool) Sampler {
	return constSampler{decision}
}

// Sample implements the Sampler interface.
func (c constSampler) Sample(uint64) bool {
	return c.decision
}

type probabilisticSampler struct {
	chance float64
	rng    *rand.Rand
}

// NewProbabilisticSampler returns a sampler that samples spans with a
// certain chance, which should be in [0, 1].
func NewProbabilisticSampler(chance float64) Sampler {
	return probabilisticSampler{chance, rand.New(rand.NewSource(time.Now().UnixNano()))}
}

// Sample implements the Sampler interface.
func (p probabilisticSampler) Sample(uint64) bool {
	return p.rng.Float64() < p.chance
}

type rateLimiter struct {
	mu     sync.Mutex
	rate   int
	tokens int
	t      time.Time
	nowFn  func() time.Time
}

func newRateLimiter(rate int) *rateLimiter {
	return &rateLimiter{
		rate:   rate,
		tokens: rate,
		t:      time.Now().Add(time.Second),
		nowFn:  time.Now,
	}
}

func (r *rateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	add := int((float64(r.nowFn().Sub(r.t).Nanoseconds()/int64(time.Millisecond)) / 1000) * float64(r.rate))
	if add > 0 {
		r.tokens = r.tokens + add
		if r.tokens > r.rate {
			r.tokens = r.rate
		}
		r.t = r.nowFn()
	}
	if r.tokens > 0 {
		r.tokens--
		return true
	}
	return false
}

type rateSampler struct {
	l *rateLimiter
}

// NewRateSampler returns a sampler that samples up to n samples per
// second.
func NewRateSampler(n int) Sampler {
	return rateSampler{newRateLimiter(n)}
}

func (r rateSampler) Sample(uint64) bool {
	return r.l.Allow()
}
