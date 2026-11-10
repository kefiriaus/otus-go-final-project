package aggregate

import (
	"testing"
	"time"
)

func at(seconds float64) time.Time {
	return time.Unix(1000, 0).Add(time.Duration(seconds * float64(time.Second))).UTC()
}
func TestWindow(t *testing.T) {
	w, err := NewWindow(at(20).In(time.FixedZone("offset", 3600)), 15, 3600)
	if err != nil || !w.Start.Equal(at(5)) || !w.End.Equal(at(20)) || w.End.Location() != time.UTC {
		t.Fatalf("window: %+v %v", w, err)
	}
	for _, v := range [][2]int{{0, 3600}, {-1, 3600}, {3601, 3600}, {1, 0}, {1, 3601}, {2, 1}} {
		if _, err := NewWindow(at(20), v[0], v[1]); err == nil {
			t.Fatalf("accepted %v", v)
		}
	}
}
func TestSchedule(t *testing.T) {
	s, err := NewSchedule(at(0), 5, 15, 3600)
	if err != nil {
		t.Fatal(err)
	}
	if !s.First.Equal(at(15)) {
		t.Fatal(s)
	}
	for _, v := range [][2]float64{{0, 15}, {14.9, 15}, {15, 20}, {20, 25}, {101, 105}} {
		if got := s.NextAfter(at(v[0])); !got.Equal(at(v[1])) {
			t.Fatalf("next %v: %v", v, got)
		}
	}
	w := s.WindowAt(at(20))
	if !w.Start.Equal(at(5)) || !w.End.Equal(at(20)) {
		t.Fatal(w)
	}
	if _, err := NewSchedule(at(0), 3600, 1, 1); err != nil {
		t.Fatal(err)
	}
	for _, v := range [][3]int{{0, 1, 3600}, {-1, 1, 3600}, {3601, 1, 3600}, {1, 0, 3600}, {1, -1, 3600}, {1, 2, 1}, {1, 1, 0}, {1, 1, 3601}} {
		if _, err := NewSchedule(at(0), v[0], v[1], v[2]); err == nil {
			t.Fatalf("accepted %v", v)
		}
	}
}

func TestWindowRejectsUnsupportedRange(t *testing.T) {
	for _, w := range []Window{{at(0), at(3601)}, {at(0), at(0).AddDate(500, 0, 0)}} {
		if w.validate() == nil {
			t.Fatal("accepted unsupported window")
		}
	}
}
func TestScheduleDistantDeadline(t *testing.T) {
	s, _ := NewSchedule(at(0), 5, 15, 3600)
	now := s.First.AddDate(500, 0, 0).Add(123 * time.Millisecond)
	want := now.Truncate(5 * time.Second).Add(5 * time.Second)
	if got := s.NextAfter(now); !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
