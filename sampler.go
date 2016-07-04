package tracer

import (
	"math/rand"
	"time"

	"golang.org/x/time/rate"
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

type rateSampler struct {
	l *rate.Limiter
}

// NewRateSampler returns a sampler that samples up to n samples per
// second.
func NewRateSampler(n int) Sampler {
	return rateSampler{rate.NewLimiter(rate.Limit(n), n)}
}

func (r rateSampler) Sample(uint64) bool {
	return r.l.Allow()
}
