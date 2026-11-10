package aggregate

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
)

func TestGaugeSeries(t *testing.T) {
	s, _ := NewGaugeSeries[int](4)
	for _, e := range []Event[int]{{At: at(0), Value: 10}, {At: at(.5), Err: errors.New("failed")}, {At: at(1), Value: 20, Partial: true}} {
		if err := s.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Read(Window{at(0), at(4)})
	if err != nil || len(got) != 2 || !got[0].End.Equal(at(.5)) || !got[1].End.Equal(at(2)) || !got[1].Partial {
		t.Fatalf("%+v %v", got, err)
	}
	clipped, _ := s.Read(Window{at(1), at(1.25)})
	if len(clipped) != 1 || !clipped[0].End.Equal(at(1.25)) {
		t.Fatal(clipped)
	}
	again, _ := s.Read(Window{at(0), at(4)})
	if !reflect.DeepEqual(got, again) {
		t.Fatal("read changed pending")
	}
	if s.Add(Event[int]{At: at(1)}) == nil || s.Add(Event[int]{At: at(0)}) == nil || s.Add(Event[int]{}) == nil {
		t.Fatal("invalid timestamp accepted")
	}
	if _, err := s.Read(Window{}); err == nil {
		t.Fatal("invalid window")
	}
	if _, err := NewGaugeSeries[int](0); err == nil {
		t.Fatal("invalid limit")
	}
}
func TestCounterSeries(t *testing.T) {
	s, _ := NewCounterSeries(4)
	values := []uint64{math.MaxUint64 - 100, 0}
	if err := s.Add(Event[CounterPoint]{At: at(0), Value: CounterPoint{"one", values}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	values[0] = 0
	initial, _ := s.Read(Window{at(0), at(1)})
	if len(initial) != 0 {
		t.Fatal(initial)
	}
	_ = s.Add(Event[CounterPoint]{At: at(1), Value: CounterPoint{"one", []uint64{math.MaxUint64 - 40, 0}}})
	got, _ := s.Read(Window{at(0), at(1)})
	if len(got) != 1 || got[0].Value[0] != 60 || got[0].Value[1] != 0 || !got[0].Partial {
		t.Fatal(got)
	}
	if s.Add(Event[CounterPoint]{At: at(1), Value: CounterPoint{"one", []uint64{1}}}) == nil {
		t.Fatal("duplicate accepted")
	}
	if s.Add(Event[CounterPoint]{At: at(2), Value: CounterPoint{}}) == nil {
		t.Fatal("empty counters")
	}
	if _, err := s.Read(Window{}); err == nil {
		t.Fatal("invalid window")
	}
	if _, err := NewCounterSeries(3601); err == nil {
		t.Fatal("invalid limit")
	}
}
func TestCounterSeriesContinuity(t *testing.T) {
	for _, mode := range []string{"error", "gap", "generation", "size", "reset"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := NewCounterSeries(4)
			_ = s.Add(Event[CounterPoint]{At: at(0), Value: CounterPoint{"a", []uint64{100}}})
			next := Event[CounterPoint]{At: at(1), Value: CounterPoint{"a", []uint64{160}}}
			switch mode {
			case "error":
				_ = s.Add(Event[CounterPoint]{At: at(.5), Err: errors.New("failure")})
			case "gap":
				next.At = at(2)
			case "generation":
				next.Value.Generation = "b"
			case "size":
				next.Value.Values = []uint64{160, 0}
			case "reset":
				next.Value.Values = []uint64{1}
			}
			if err := s.Add(next); err != nil {
				t.Fatal(err)
			}
			got, _ := s.Read(Window{at(0), at(4)})
			if len(got) != 0 {
				t.Fatal(got)
			}
			recovered := CounterPoint{Generation: next.Value.Generation, Values: append([]uint64(nil), next.Value.Values...)}
			for i := range recovered.Values {
				recovered.Values[i] += 10
			}
			if err := s.Add(Event[CounterPoint]{At: next.At.Add(1e9), Value: recovered}); err != nil {
				t.Fatal(err)
			}
			got, _ = s.Read(Window{at(0), at(5)})
			if len(got) != 1 || got[0].Value[0] != 10 {
				t.Fatal(got)
			}
		})
	}
}
func TestSeriesConcurrent(t *testing.T) {
	g, _ := NewGaugeSeries[int](10)
	c, _ := NewCounterSeries(10)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if err := g.Add(Event[int]{At: at(float64(i)), Value: i}); err != nil {
				t.Error(err)
			}
			if err := c.Add(Event[CounterPoint]{At: at(float64(i)), Value: CounterPoint{Values: []uint64{uint64(i)}}}); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_, _ = g.Read(Window{at(0), at(2000)})
			_, _ = c.Read(Window{at(0), at(2000)})
		}
	}()
	wg.Wait()
	got, _ := g.Read(Window{at(0), at(2000)})
	if len(got) > 13 {
		t.Fatal("unbounded gauge history")
	}
	rates, _ := c.Read(Window{at(0), at(2000)})
	if len(rates) > 12 {
		t.Fatal("unbounded counter history")
	}
}

func TestCounterSeriesGapBoundary(t *testing.T) {
	for _, gap := range []float64{1.1, 1.100000001} {
		t.Run(fmt.Sprint(gap), func(t *testing.T) {
			s, _ := NewCounterSeries(4)
			_ = s.Add(Event[CounterPoint]{At: at(0), Value: CounterPoint{Values: []uint64{0}}})
			_ = s.Add(Event[CounterPoint]{At: at(gap), Value: CounterPoint{Values: []uint64{11}}, Partial: true})
			got, _ := s.Read(Window{at(0), at(2)})
			if gap == 1.1 {
				if len(got) != 1 || !got[0].Partial {
					t.Fatal(got)
				}
				closeTo(t, got[0].Value[0], 10)
			} else if len(got) != 0 {
				t.Fatal(got)
			}
		})
	}
}
func TestCounterSeriesResetThenError(t *testing.T) {
	s, _ := NewCounterSeries(4)
	for _, e := range []Event[CounterPoint]{{At: at(0), Value: CounterPoint{Values: []uint64{100}}}, {At: at(1), Value: CounterPoint{Values: []uint64{1}}}, {At: at(2), Err: errors.New("failed")}, {At: at(3), Value: CounterPoint{Values: []uint64{11}}}, {At: at(4), Value: CounterPoint{Values: []uint64{21}}}} {
		if err := s.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := s.Read(Window{at(0), at(4)})
	if len(got) != 1 || !got[0].Start.Equal(at(3)) || got[0].Value[0] != 10 {
		t.Fatal(got)
	}
}
