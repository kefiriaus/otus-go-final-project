package aggregate

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

// Event is a completed measurement attempt. Value is ignored when Err is set.
// At is the completion time, and must increase strictly within each series.
type Event[T any] struct {
	At      time.Time
	Value   T
	Err     error
	Partial bool
}

func validateEvent(at, last time.Time) error {
	if at.IsZero() || (!last.IsZero() && !at.After(last)) {
		return fmt.Errorf("event timestamp must be nonzero and strictly increasing")
	}
	return nil
}

// GaugeSeries holds bounded intervals plus one pending immutable gauge value.
// Construct it with NewGaugeSeries.
type GaugeSeries[T any] struct {
	mu      sync.Mutex
	history *History[T]
	last    time.Time
	pending *Event[T]
}

// NewGaugeSeries allocates a one-second gauge series with bounded history.
func NewGaugeSeries[T any](maxWindowSeconds int) (*GaugeSeries[T], error) {
	h, err := NewHistory[T](maxWindowSeconds, maxWindowSeconds+2)
	if err != nil {
		return nil, err
	}
	return &GaugeSeries[T]{history: h}, nil
}

// Add ends the preceding value at the next attempt, or after one second.
func (s *GaugeSeries[T]) Add(event Event[T]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateEvent(event.At, s.last); err != nil {
		return err
	}
	event.At = event.At.UTC()
	if s.pending != nil {
		p := s.pending
		end := p.At.Add(time.Second)
		if event.At.Before(end) {
			end = event.At
		}
		if err := s.history.Append(Interval[T]{Start: p.At, End: end, ObservedAt: p.At, Value: p.Value, Partial: p.Partial}); err != nil {
			return err
		}
	}
	s.last = event.At
	s.pending = nil
	if event.Err == nil {
		s.pending = &event
	}
	return nil
}

// Read includes the pending gauge only through its one-second validity limit.
func (s *GaugeSeries[T]) Read(window Window) ([]Interval[T], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.history.Read(window)
	if err != nil {
		return nil, err
	}
	if p := s.pending; p != nil {
		end := p.At.Add(time.Second)
		if window.End.Before(end) {
			end = window.End
		}
		v := Interval[T]{Start: p.At, End: end, ObservedAt: p.At, Value: p.Value, Partial: p.Partial}
		if v.Start.Before(v.End) && v.intersects(window) {
			result = append(result, v)
		}
	}
	return result, nil
}

// CounterPoint holds cumulative counters for one identity and generation.
// Collectors must change Generation when a reset cannot be inferred from values.
type CounterPoint struct {
	Generation string
	Values     []uint64
}

// CounterSeries converts adjacent successful counters into per-second rates.
// Construct it with NewCounterSeries. Returned interval values are immutable.
type CounterSeries struct {
	mu       sync.Mutex
	history  *History[[]float64]
	last     time.Time
	baseline *Event[CounterPoint]
}

// NewCounterSeries allocates bounded history and one counter baseline.
func NewCounterSeries(maxWindowSeconds int) (*CounterSeries, error) {
	h, err := NewHistory[[]float64](maxWindowSeconds, maxWindowSeconds+2)
	if err != nil {
		return nil, err
	}
	return &CounterSeries{history: h}, nil
}

// Add breaks continuity on errors, resets, generation changes, and long gaps.
func (s *CounterSeries) Add(event Event[CounterPoint]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateEvent(event.At, s.last); err != nil {
		return err
	}
	if event.Err == nil && len(event.Value.Values) == 0 {
		return fmt.Errorf("counter vector must not be empty")
	}
	event.At = event.At.UTC()
	if event.Err != nil {
		s.last = event.At
		s.baseline = nil
		return nil
	}
	if previous := s.baseline; previous != nil {
		duration := event.At.Sub(previous.At)
		valid := duration <= 1100*time.Millisecond && event.Value.Generation == previous.Value.Generation && len(event.Value.Values) == len(previous.Value.Values)
		if valid {
			for i, value := range event.Value.Values {
				if value < previous.Value.Values[i] {
					valid = false
					break
				}
			}
		}
		if valid {
			rates := make([]float64, len(event.Value.Values))
			for i, value := range event.Value.Values {
				rates[i] = float64(value-previous.Value.Values[i]) / duration.Seconds()
			}
			v := Interval[[]float64]{Start: previous.At, End: event.At, ObservedAt: event.At, Value: rates, Partial: previous.Partial || event.Partial}
			if err := s.history.Append(v); err != nil {
				return err
			}
		}
	}
	event.Value.Values = slices.Clone(event.Value.Values)
	s.baseline = &event
	s.last = event.At
	return nil
}

// Read selects immutable rate intervals without consuming them.
func (s *CounterSeries) Read(window Window) ([]Interval[[]float64], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.history.Read(window)
}
