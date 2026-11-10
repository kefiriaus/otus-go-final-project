package aggregate

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func interval[T any](start, end float64, value T) Interval[T] {
	return Interval[T]{Start: at(start), End: at(end), ObservedAt: at(start), Value: value}
}
func TestHistory(t *testing.T) {
	h, err := NewHistory[int](2, 4)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		v := interval(float64(i), float64(i+1), i)
		v.Start = v.Start.In(time.FixedZone("x", 3600))
		if err := h.Append(v); err != nil {
			t.Fatal(err)
		}
	}
	all, err := h.Read(Window{at(0), at(20)})
	if err != nil || len(all) != 4 || all[0].Value != 6 || all[0].Start.Location() != time.UTC {
		t.Fatalf("%+v %v", all, err)
	}
	w := Window{at(7), at(8)}
	one, _ := h.Read(w)
	if len(one) != 1 || one[0].Value != 7 {
		t.Fatal(one)
	}
	one[0].Value = 999
	again, _ := h.Read(w)
	if again[0].Value != 7 {
		t.Fatal("read aliases storage")
	}
	before, _ := h.Read(Window{at(0), at(20)})
	for _, bad := range []Interval[int]{interval(9, 11, 1), interval(11, 11, 1), interval(11, 10, 1), {Start: at(11), End: at(12)}} {
		if h.Append(bad) == nil {
			t.Fatal("accepted invalid interval")
		}
	}
	after, _ := h.Read(Window{at(0), at(20)})
	if !reflect.DeepEqual(before, after) {
		t.Fatal("invalid append mutated history")
	}
	for _, w := range []Window{{}, {at(2), at(1)}, {at(1), at(1)}} {
		if _, err := h.Read(w); err == nil {
			t.Fatal("invalid window")
		}
	}
}
func TestHistoryRetention(t *testing.T) {
	h, _ := NewHistory[int](1, 20)
	for i := 0; i < 10; i++ {
		if err := h.Append(interval(float64(i), float64(i+1), i)); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := h.Read(Window{at(0), at(20)})
	if len(got) != 3 || got[0].Value != 7 {
		t.Fatal(got)
	}
	dense, _ := NewHistory[int](3600, 2)
	for i := 0; i < 100; i++ {
		_ = dense.Append(interval(float64(i)/100, float64(i+1)/100, i))
	}
	got, _ = dense.Read(Window{at(0), at(2)})
	if len(got) != 2 || got[0].Value != 98 {
		t.Fatal(got)
	}
	for _, v := range [][2]int{{0, 1}, {3601, 1}, {1, 0}, {1, -1}} {
		if _, err := NewHistory[int](v[0], v[1]); err == nil {
			t.Fatal(v)
		}
	}
}
func TestHistoryConcurrent(t *testing.T) {
	h, _ := NewHistory[int](10, 12)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if err := h.Append(interval(float64(i), float64(i+1), i)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if _, err := h.Read(Window{at(0), at(2000)}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
}
