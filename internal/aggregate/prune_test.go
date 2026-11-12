package aggregate

import (
	"testing"
	"time"
)

func TestPruneBefore(t *testing.T) {
	at := time.Unix(100, 0).UTC()
	h, _ := NewHistory[*int](10, 12)
	n := 1
	if err := h.Append(Interval[*int]{Start: at, End: at.Add(10 * time.Second), ObservedAt: at, Value: &n}); err != nil {
		t.Fatal(err)
	}
	if err := h.PruneBefore(at.Add(4 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := h.Read(Window{at, at.Add(10 * time.Second)})
	if err != nil || len(got) != 1 || !got[0].Start.Equal(at.Add(4*time.Second)) {
		t.Fatalf("%v %v", got, err)
	}
	if err := h.PruneBefore(at.Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, v := range h.entries {
		if v.Value != nil {
			t.Fatal("retained pruned pointer")
		}
	}
	if h.PruneBefore(time.Time{}) == nil {
		t.Fatal("zero cutoff accepted")
	}
}
func TestSeriesPruning(t *testing.T) {
	at := time.Unix(100, 0).UTC()
	s, _ := NewCounterSeries(10)
	s.Add(Event[CounterPoint]{At: at, Value: CounterPoint{Values: []uint64{1}}})
	if err := s.PruneBefore(at); err != nil {
		t.Fatal(err)
	}
	s.Add(Event[CounterPoint]{At: at.Add(time.Second), Value: CounterPoint{Values: []uint64{3}}})
	got, _ := s.Read(Window{at, at.Add(time.Second)})
	if len(got) != 1 || got[0].Value[0] != 2 {
		t.Fatalf("baseline lost: %v", got)
	}
	g, _ := NewGaugeSeries[int](10)
	g.Add(Event[int]{At: at, Value: 1})
	if err := g.PruneBefore(at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	gotGauge, _ := g.Read(Window{at, at.Add(2 * time.Second)})
	if len(gotGauge) != 0 {
		t.Fatal("pending value retained")
	}
}
