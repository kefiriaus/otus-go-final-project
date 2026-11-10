package aggregate

import (
	"fmt"
	"math"
	"time"
)

// Summary distinguishes unavailable measurements from measured zero.
// A nonnil Data value is partial if time coverage is incomplete or input was partial.
type Summary[T any] struct {
	Data          *T
	CoverageRatio float64
	LastSampleAt  *time.Time
	Partial       bool
}

type selection struct {
	durations []time.Duration
	covered   time.Duration
	last      time.Time
	partial   bool
}

func selectIntervals[T any](window Window, values []Interval[T]) (selection, error) {
	if err := window.validate(); err != nil {
		return selection{}, err
	}
	result := selection{durations: make([]time.Duration, len(values))}
	for i, v := range values {
		if err := v.validate(); err != nil {
			return selection{}, err
		}
		if i > 0 && v.Start.Before(values[i-1].End) {
			return selection{}, fmt.Errorf("intervals must be ordered and nonoverlapping")
		}
		if !v.intersects(window) {
			continue
		}
		start, end := v.Start, v.End
		if start.Before(window.Start) {
			start = window.Start
		}
		if end.After(window.End) {
			end = window.End
		}
		d := end.Sub(start)
		result.durations[i] = d
		result.covered += d
		result.partial = result.partial || v.Partial
		if v.ObservedAt.After(result.last) {
			result.last = v.ObservedAt
		}
	}
	return result, nil
}
func summarized[T any](window Window, s selection, value T, last time.Time) Summary[T] {
	timestamp := last.UTC()
	coverage := float64(s.covered) / float64(window.End.Sub(window.Start))
	return Summary[T]{Data: &value, CoverageRatio: coverage, LastSampleAt: &timestamp, Partial: s.partial || s.covered < window.End.Sub(window.Start)}
}
func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

// WeightedMean averages a gauge over usable time, excluding uncovered gaps.
func WeightedMean(window Window, values []Interval[float64]) (Summary[float64], error) {
	s, err := selectIntervals(window, values)
	if err != nil {
		return Summary[float64]{}, err
	}
	result := 0.0
	for i, v := range values {
		if !finite(v.Value) {
			return Summary[float64]{}, fmt.Errorf("mean requires finite values")
		}
		if s.durations[i] == 0 {
			continue
		}
		result += v.Value * (float64(s.durations[i]) / float64(s.covered))
		if !finite(result) {
			return Summary[float64]{}, fmt.Errorf("mean overflow")
		}
	}
	if s.covered == 0 {
		return Summary[float64]{}, nil
	}
	return summarized(window, s, result, s.last), nil
}

// MeanRates averages nonnegative per-second rate vectors over usable time.
// CPU percentages require their own counter-delta calculation, not this reducer.
func MeanRates(window Window, values []Interval[[]float64]) (Summary[[]float64], error) {
	s, err := selectIntervals(window, values)
	if err != nil {
		return Summary[[]float64]{}, err
	}
	var result []float64
	for i, v := range values {
		if len(v.Value) == 0 || (i > 0 && len(v.Value) != len(values[0].Value)) {
			return Summary[[]float64]{}, fmt.Errorf("rate vectors require equal nonzero lengths")
		}
		if result == nil {
			result = make([]float64, len(v.Value))
		}
		for j, value := range v.Value {
			if !finite(value) || value < 0 {
				return Summary[[]float64]{}, fmt.Errorf("rates must be finite and nonnegative")
			}
			if s.durations[i] == 0 {
				continue
			}
			result[j] += value * (float64(s.durations[i]) / float64(s.covered))
			if !finite(result[j]) {
				return Summary[[]float64]{}, fmt.Errorf("rate mean overflow")
			}
		}
	}
	if s.covered == 0 {
		return Summary[[]float64]{}, nil
	}
	return summarized(window, s, result, s.last), nil
}

// Latest returns the last state observed strictly before the window end.
// Coverage describes periodic availability, not averaging of state values.
func Latest[T any](window Window, values []Interval[T]) (Summary[T], error) {
	s, err := selectIntervals(window, values)
	if err != nil {
		return Summary[T]{}, err
	}
	selected := -1
	for i, v := range values {
		if s.durations[i] > 0 && v.ObservedAt.Before(window.End) && (selected < 0 || v.ObservedAt.After(values[selected].ObservedAt)) {
			selected = i
		}
	}
	if selected < 0 {
		return Summary[T]{}, nil
	}
	return summarized(window, s, values[selected].Value, values[selected].ObservedAt), nil
}
