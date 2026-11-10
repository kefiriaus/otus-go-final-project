package aggregate

import (
	"fmt"
	"sync"
	"time"
)

// Interval describes when an immutable value is usable.
type Interval[T any] struct {
	Start      time.Time
	End        time.Time
	ObservedAt time.Time
	Value      T
	Partial    bool
}

func (v Interval[T]) validate() error {
	if v.Start.IsZero() || v.End.IsZero() || v.ObservedAt.IsZero() || !v.Start.Before(v.End) {
		return fmt.Errorf("interval requires nonzero timestamps and positive duration")
	}
	return nil
}
func (v Interval[T]) utc() Interval[T] {
	v.Start = v.Start.UTC()
	v.End = v.End.UTC()
	v.ObservedAt = v.ObservedAt.UTC()
	return v
}
func (v Interval[T]) intersects(w Window) bool { return v.Start.Before(w.End) && v.End.After(w.Start) }

// History is a bounded, concurrent ring of ordered, nonoverlapping intervals.
// Values and nested storage must remain immutable after Append and Read.
// Construct it with NewHistory; the zero value is not usable.
type History[T any] struct {
	mu        sync.RWMutex
	entries   []Interval[T]
	head      int
	size      int
	retention time.Duration
}

// NewHistory allocates fixed entry storage and a two-second retention allowance.
func NewHistory[T any](maxWindowSeconds, maxEntries int) (*History[T], error) {
	if maxWindowSeconds < 1 || maxWindowSeconds > 3600 || maxEntries <= 0 {
		return nil, fmt.Errorf("history requires window limit 1..3600 and positive capacity")
	}
	return &History[T]{entries: make([]Interval[T], maxEntries), retention: time.Duration(maxWindowSeconds+2) * time.Second}, nil
}
func (h *History[T]) drop() {
	var zero Interval[T]
	h.entries[h.head] = zero
	h.head = (h.head + 1) % len(h.entries)
	h.size--
}

// Append rejects invalid ordering without modifying the ring.
func (h *History[T]) Append(value Interval[T]) error {
	if err := value.validate(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.size > 0 {
		last := h.entries[(h.head+h.size-1)%len(h.entries)]
		if value.Start.Before(last.End) {
			return fmt.Errorf("interval overlaps or precedes history")
		}
	}
	cutoff := value.End.Add(-h.retention)
	for h.size > 0 && !h.entries[h.head].End.After(cutoff) {
		h.drop()
	}
	if h.size == len(h.entries) {
		h.drop()
	}
	h.entries[(h.head+h.size)%len(h.entries)] = value.utc()
	h.size++
	return nil
}

// Read copies interval records intersecting window; it never consumes entries.
func (h *History[T]) Read(window Window) ([]Interval[T], error) {
	if err := window.validate(); err != nil {
		return nil, err
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	var result []Interval[T]
	for i := 0; i < h.size; i++ {
		v := h.entries[(h.head+i)%len(h.entries)]
		if v.intersects(window) {
			result = append(result, v)
		}
	}
	return result, nil
}
