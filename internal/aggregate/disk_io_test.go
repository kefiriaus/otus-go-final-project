package aggregate

import (
	"math"
	"reflect"
	"testing"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const testDiskIdentity = "8:0"

func TestDiskIOCounterPoint(t *testing.T) {
	got := DiskIOCounterPoint(
		metrics.DiskIOCounters{
			Identity:        "259:0",
			ReadsCompleted:  math.MaxUint64,
			WritesCompleted: 2,
			SectorsRead:     math.MaxUint64 - 1,
			SectorsWritten:  4,
		},
	)
	if got.Generation != "259:0" ||
		!reflect.DeepEqual(got.Values, []uint64{math.MaxUint64, 2, math.MaxUint64 - 1, 4}) {
		t.Fatal(got)
	}
	for component := 0; component < 4; component++ {
		s, _ := NewCounterSeries(4)
		_ = s.Add(
			Event[CounterPoint]{
				At: at(0),
				Value: CounterPoint{
					Generation: testDiskIdentity,
					Values:     []uint64{100, 100, 100, 100},
				},
			},
		)
		baseline, _ := s.Read(Window{at(0), at(1)})
		if len(baseline) != 0 {
			t.Fatal("baseline produced rate")
		}
		next := CounterPoint{Generation: testDiskIdentity, Values: []uint64{110, 110, 110, 110}}
		next.Values[component] = 99
		_ = s.Add(Event[CounterPoint]{At: at(1), Value: next})
		values, _ := s.Read(Window{at(0), at(1)})
		if len(values) != 0 {
			t.Fatal("raw decrease retained")
		}
		for i := range next.Values {
			next.Values[i] += 10
		}
		_ = s.Add(Event[CounterPoint]{At: at(2), Value: next, Partial: true})
		values, _ = s.Read(Window{at(0), at(2)})
		got, err := MeanDiskIO(Window{at(0), at(2)}, "sda", values)
		if err != nil || got.Data == nil || !got.Partial || got.CoverageRatio != .5 ||
			!got.LastSampleAt.Equal(at(2)) {
			t.Fatalf("%+v %v", got, err)
		}
		closeTo(t, got.Data.TransfersPerSecond, 20)
		closeTo(t, got.Data.KibibytesPerSecond, 10)
	}
	s, _ := NewCounterSeries(4)
	for i, identity := range []string{testDiskIdentity, "8:16", "8:16"} {
		_ = s.Add(
			Event[CounterPoint]{
				At: at(float64(i)),
				Value: DiskIOCounterPoint(
					metrics.DiskIOCounters{Identity: identity, ReadsCompleted: uint64(i) * 10},
				),
			},
		)
	}
	values, _ := s.Read(Window{at(0), at(2)})
	if len(values) != 1 || !values[0].Start.Equal(at(1)) {
		t.Fatal(values)
	}
}

func TestMeanDiskIO(t *testing.T) {
	v := interval(0, 1, []float64{10, 5, 20, 10})
	v.ObservedAt = at(1)
	got, err := MeanDiskIO(Window{at(0), at(1)}, "sda", []Interval[[]float64]{v})
	if err != nil || got.Data == nil || got.Data.Device != "sda" || got.CoverageRatio != 1 ||
		got.Partial ||
		!got.LastSampleAt.Equal(at(1)) {
		t.Fatalf("%+v %v", got, err)
	}
	closeTo(t, got.Data.TransfersPerSecond, 15)
	closeTo(t, got.Data.KibibytesPerSecond, 15)
	odd, err := MeanDiskIO(
		Window{at(0), at(1)},
		"nvme0n1",
		[]Interval[[]float64]{interval(0, 1, []float64{0, 0, 1, 0})},
	)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, odd.Data.KibibytesPerSecond, .5)
	zero := interval(0, 1, make([]float64, 4))
	zero.Partial = true
	measured, err := MeanDiskIO(Window{at(0), at(1)}, "loop0", []Interval[[]float64]{zero})
	if err != nil || measured.Data == nil || measured.Data.TransfersPerSecond != 0 ||
		measured.Data.KibibytesPerSecond != 0 ||
		measured.CoverageRatio != 1 ||
		!measured.Partial {
		t.Fatalf("%+v %v", measured, err)
	}
	empty, err := MeanDiskIO(Window{at(0), at(1)}, "sda", nil)
	if err != nil || empty.Data != nil || empty.LastSampleAt != nil || empty.CoverageRatio != 0 {
		t.Fatalf("%+v %v", empty, err)
	}
	values := []Interval[[]float64]{
		interval(0, 1, []float64{10, 0, 20, 0}),
		interval(1, 3, []float64{20, 0, 40, 0}),
	}
	mean, err := MeanDiskIO(Window{at(0), at(4)}, "sda", values)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, mean.Data.TransfersPerSecond, 50.0/3)
	closeTo(t, mean.Data.KibibytesPerSecond, 50.0/3)
	closeTo(t, mean.CoverageRatio, .75)
	clipped, err := MeanDiskIO(Window{at(.5), at(1.5)}, "sda", values)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, clipped.Data.TransfersPerSecond, 15)
	other, err := MeanDiskIO(
		Window{at(0), at(4)},
		"sdb",
		[]Interval[[]float64]{interval(2, 3, []float64{100, 0, 0, 0})},
	)
	if err != nil || other.Data.Device == mean.Data.Device || other.CoverageRatio != .25 {
		t.Fatalf("%+v %v", other, err)
	}
	closeTo(t, other.Data.TransfersPerSecond, 100)
}

func TestMeanDiskIOClippedObservation(t *testing.T) {
	v := interval(0, 1.05, []float64{10, 5, 20, 10})
	v.ObservedAt = at(1.05)
	got, err := MeanDiskIO(Window{at(0), at(1)}, "sda", []Interval[[]float64]{v})
	if err != nil || got.Data == nil {
		t.Fatalf("%+v %v", got, err)
	}
	if got.LastSampleAt == nil || !got.LastSampleAt.Equal(at(1)) {
		t.Fatalf("last sample %v, want %v", got.LastSampleAt, at(1))
	}
	closeTo(t, got.Data.TransfersPerSecond, 15)
}

func TestMeanDiskIOInvalid(t *testing.T) {
	if _, err := MeanDiskIO(Window{}, "sda", nil); err == nil {
		t.Fatal("invalid window")
	}
	if _, err := MeanDiskIO(Window{at(0), at(1)}, "", nil); err == nil {
		t.Fatal("empty device")
	}
	for _, values := range [][]Interval[[]float64]{
		{interval(0, 1, []float64{})},
		{interval(0, 1, []float64{1})},
		{interval(0, 1, make([]float64, 5))},
		{interval(1, 0, make([]float64, 4))},
		{interval(0, 2, make([]float64, 4)), interval(1, 3, make([]float64, 4))},
	} {
		if _, err := MeanDiskIO(Window{at(0), at(4)}, "sda", values); err == nil {
			t.Fatal("invalid intervals or vector")
		}
	}
	for i := 0; i < 4; i++ {
		for _, bad := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
			rates := make([]float64, 4)
			rates[i] = bad
			if _, err := MeanDiskIO(
				Window{at(0), at(1)},
				"sda",
				[]Interval[[]float64]{interval(0, 1, rates)},
			); err == nil {
				t.Fatal("invalid rate")
			}
		}
	}
	if _, err := MeanDiskIO(
		Window{at(0), at(1)},
		"sda",
		[]Interval[[]float64]{interval(0, 1, []float64{math.MaxFloat64, math.MaxFloat64, 0, 0})},
	); err == nil {
		t.Fatal("overflow accepted")
	}
	huge, err := MeanDiskIO(
		Window{at(0), at(1)},
		"sda",
		[]Interval[[]float64]{interval(0, 1, []float64{0, 0, math.MaxFloat64, math.MaxFloat64})},
	)
	if err != nil || huge.Data == nil || huge.Data.KibibytesPerSecond != math.MaxFloat64 {
		t.Fatalf("avoidable overflow: %+v %v", huge, err)
	}
}
