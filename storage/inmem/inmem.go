// Package inmem provides in-memory storage. It's probably only useful for quick
// demos and proofs of concept.
package inmem

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/tracer/tracer"
	"github.com/tracer/tracer/server"
)

func init() {
	server.RegisterStorage("inmem", setup)
}

func setup(map[string]interface{}) (server.Storage, error) {
	return &Inmem{
		spans: map[uint64]tracer.RawSpan{},
	}, nil
}

var _ server.Storage = &Inmem{}

// Inmem is a storage that keeps everything in memory.
type Inmem struct {
	mtx   sync.RWMutex
	spans map[uint64]tracer.RawSpan
}

var (
	// ErrNotFound is returned when a requested ID is not in storage.
	ErrNotFound = errors.New("not found")
)

// Store implements the server.Storage interface.
func (s *Inmem) Store(sp tracer.RawSpan) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.spans[sp.SpanID] = sp
	return nil
}

// TraceByID implements the server.Storage interface.
func (s *Inmem) TraceByID(id uint64) (tracer.RawTrace, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var spans []tracer.RawSpan
	var relations []tracer.RawRelation
	for _, sp := range s.spans {
		if sp.TraceID == id {
			spans = append(spans, sp)
			relations = append(relations, tracer.RawRelation{
				ParentID: sp.ParentID,
				ChildID:  sp.SpanID,
				Kind:     "parent", // TODO(pb): is this right?
			})
		}
	}
	if len(spans) <= 0 {
		return tracer.RawTrace{}, ErrNotFound
	}
	return tracer.RawTrace{
		TraceID:   id,
		Spans:     spans,
		Relations: relations,
	}, nil
}

// SpanByID implements the server.Storage interface.
func (s *Inmem) SpanByID(id uint64) (tracer.RawSpan, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	sp, ok := s.spans[id]
	if !ok {
		return tracer.RawSpan{}, ErrNotFound
	}
	return sp, nil
}

// QueryTraces implements the server.Storage interface.
func (s *Inmem) QueryTraces(q server.Query) ([]tracer.RawTrace, error) {
	var filters []func(tracer.RawTrace) bool
	if !q.StartTime.IsZero() {
		filters = append(filters, filterStartTime(q.StartTime))
	}
	if !q.FinishTime.IsZero() {
		filters = append(filters, filterFinishTime(q.FinishTime))
	}
	if q.OperationName != "" {
		filters = append(filters, filterOperationName(q.OperationName))
	}
	if q.MinDuration > 0 {
		filters = append(filters, filterMinDuration(q.MinDuration))
	}
	if q.MaxDuration > 0 {
		filters = append(filters, filterMaxDuration(q.MaxDuration))
	}
	if len(q.AndTags) > 0 {
		filters = append(filters, filterAndTags(q.AndTags))
	}
	if len(q.OrTags) > 0 {
		filters = append(filters, filterOrTags(q.OrTags))
	}
	if q.Num > 0 {
		filters = append(filters, filterNum(q.Num))
	}

	for _, sp := range s.spans {

	}

}

// Services implements the server.Storage interface.
func (Inmem) Services() ([]string, error) { return nil, nil }

// Spans implements the server.Storage interface.
func (Inmem) Operations(service string) ([]string, error) { return nil, nil }

// Dependencies implements the server.Storage interface.
func (Inmem) Dependencies() ([]server.Dependency, error) { return nil, nil }

func filterStartTime(t time.Time) func(tracer.RawTrace) bool {
	return func(tr tracer.RawTrace) bool {
		// The earliest span in this trace must start after t.
		earliest := time.Now()
		for _, sp := range tr.Spans {
			if sp.StartTime.Before(earliest) {
				earliest = sp.StartTime
			}
		}
		return earliest.After(t)
	}
}

func filterFinishTime(t time.Time) func(tracer.RawTrace) bool {
	return func(tr tracer.RawTrace) bool {
		// The oldest span in this trace must start before t.
		var oldest time.Time
		for _, sp := range tr.Spans {
			if sp.FinishTime.After(oldest) {
				oldest = sp.FinishTime
			}
		}
		return oldest.Before(t)
	}
}

func filterOperationName(s string) func(tracer.RawTrace) bool {
	return func(tr tracer.RawTrace) bool {
		// Some span in this trace must have an OperationName that's EqualFold
		// to the given operation.
		for _, sp := range tr.Spans {
			if strings.EqualFold(sp.OperationName, s) {
				return true
			}
		}
		return false
	}
}

func filterMinDuration(d time.Duration) func(tracer.RawTrace) bool {
	return func(tr tracer.RawTrace) bool {
		// Some span must have a difference between its start and finish time
		// that's bigger than the minimum duration d.
		// TODO(pb): is this right?
		for _, sp := range tr.Spans {
			if sp.FinishTime.Sub(sp.StartTime) > d {
				return true
			}
		}
		return false
	}
}

func filterMaxDuration(d time.Duration) func(tracer.RawTrace) bool {
	return func(tr tracer.RawTrace) bool {
		// Some span must have a difference between its start and finish time
		// that's smaller than the maximum duration d.
		// TODO(pb): is this right?
		for _, sp := range tr.Spans {
			if sp.FinishTime.Sub(sp.StartTime) < d {
				return true
			}
		}
		return false
	}
}

func filterAndTags(tags []server.QueryTag) func(tracer.RawTrace) bool {
	unchecked := map[string]struct{}{}
	checked := map[string]string{}
	for _, tag := range tags {
		if tag.CheckValue {
			checked[tag.Key] = tag.Value
		} else {
			unchecked[tag.Key] = struct{}{}
		}
	}

	return func(tr tracer.RawTrace) bool {
		// Return true if, after walking all spans in the trace,
		// all of the provided tags have been matched.
		// TODO(pb): is this right?
		for _, sp := range tr.Spans {
			for tag, x := range sp.Tags {
				if _, ok := unchecked[tag]; ok {
					delete(unchecked, tag)
				}
				s, ok := x.(string) // TODO(pb): is this right?
				if !ok {
					continue
				}
				if v, ok := checked[tag]; ok && v == s {
					delete(checked, tag)
				}
			}
		}
		return len(unchecked) == 0 && len(checked) == 0
	}
}

func filterOrTags(tags []server.QueryTag) func(tracer.RawTrace) bool {
	unchecked := map[string]struct{}{}
	checked := map[string]string{}
	for _, tag := range tags {
		if tag.CheckValue {
			checked[tag.Key] = tag.Value
		} else {
			unchecked[tag.Key] = struct{}{}
		}
	}

	return func(tr tracer.RawTrace) bool {
		// Return true the moment that any span in the trace
		// contains any of the provided tags.
		// TODO(pb): is this right?
		for _, sp := range tr.Spans {
			for tag, x := range sp.Tags {
				if _, ok := unchecked[tag]; ok {
					return true
				}
				s, ok := x.(string) // TODO(pb): is this right?
				if !ok {
					continue
				}
				if v, ok := checked[tag]; ok && v == s {
					return true
				}
			}
		}
		return false
	}
}

func filterNum(n int) func(tracer.RawTrace) bool {
	var count int
	return func(tr tracer.RawTrace) bool {
		count++
		return count <= n
	}
}
