package aggregate

import (
	"math"
	"reflect"
	"testing"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func cpuRates(user, idle float64) []float64 { return []float64{user, 0, 0, idle, 0, 0, 0, 0} }
func TestCPUCounterPoint(t *testing.T) {
	value := metrics.CPUCounters{
		User:    math.MaxUint64,
		Nice:    2,
		System:  3,
		Idle:    4,
		IOWait:  5,
		IRQ:     6,
		SoftIRQ: 7,
		Steal:   8,
	}
	got := CPUCounterPoint(value)
	if got.Generation != "" ||
		!reflect.DeepEqual(got.Values, []uint64{math.MaxUint64, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatal(got)
	}
}

func TestCPUCounterPointContinuity(t *testing.T) {
	for component := 0; component < 8; component++ {
		s, _ := NewCounterSeries(4)
		start := CounterPoint{Values: []uint64{100, 100, 100, 100, 100, 100, 100, 100}}
		if err := s.Add(Event[CounterPoint]{At: at(0), Value: start}); err != nil {
			t.Fatal(err)
		}
		first, _ := s.Read(Window{at(0), at(1)})
		if len(first) != 0 {
			t.Fatal("baseline produced data")
		}
		next := CounterPoint{Values: []uint64{110, 110, 110, 110, 110, 110, 110, 110}}
		next.Values[component] = 99
		_ = s.Add(Event[CounterPoint]{At: at(1), Value: next})
		excluded, _ := s.Read(Window{at(0), at(1)})
		if len(excluded) != 0 {
			t.Fatalf("decrease in component %d retained", component)
		}
		recovered := CounterPoint{Values: append([]uint64(nil), next.Values...)}
		for i := range recovered.Values {
			recovered.Values[i] += 10
		}
		_ = s.Add(Event[CounterPoint]{At: at(2), Value: recovered, Partial: true})
		values, _ := s.Read(Window{at(0), at(2)})
		summary, err := MeanCPUUsage(Window{at(0), at(2)}, values)
		if err != nil || summary.Data == nil || summary.CoverageRatio != .5 || !summary.Partial ||
			!summary.LastSampleAt.Equal(at(2)) {
			t.Fatalf("%+v %v", summary, err)
		}
	}
}

func TestMeanCPUUsage(t *testing.T) {
	fixture := []float64{20, 10, 15, 40, 5, 3, 2, 5}
	v := interval(0, 1, fixture)
	v.ObservedAt = at(1)
	got, err := MeanCPUUsage(Window{at(0), at(1)}, []Interval[[]float64]{v})
	if err != nil || got.Data == nil || got.CoverageRatio != 1 || got.Partial ||
		!got.LastSampleAt.Equal(at(1)) {
		t.Fatalf("%+v %v", got, err)
	}
	want := metrics.CPUUsage{
		UserPercent:   30,
		SystemPercent: 20,
		IdlePercent:   40,
		IOWaitPercent: 5,
		StealPercent:  5,
	}
	for i, value := range []float64{
		got.Data.UserPercent, got.Data.SystemPercent, got.Data.IdlePercent,
		got.Data.IOWaitPercent, got.Data.StealPercent,
	} {
		closeTo(
			t,
			value,
			[]float64{want.UserPercent, want.SystemPercent, want.IdlePercent, want.IOWaitPercent, want.StealPercent}[i],
		)
	}
	if !reflect.DeepEqual(fixture, []float64{20, 10, 15, 40, 5, 3, 2, 5}) {
		t.Fatal("mutated input")
	}
	v.Partial = true
	got, err = MeanCPUUsage(Window{at(0), at(1)}, []Interval[[]float64]{v})
	if err != nil || !got.Partial || got.CoverageRatio != 1 {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestMeanCPUUsageClippedObservation(t *testing.T) {
	v := interval(0, 1.05, cpuRates(10, 90))
	v.ObservedAt = at(1.05)
	got, err := MeanCPUUsage(Window{at(0), at(1)}, []Interval[[]float64]{v})
	if err != nil || got.Data == nil {
		t.Fatalf("%+v %v", got, err)
	}
	if got.LastSampleAt == nil || !got.LastSampleAt.Equal(at(1)) {
		t.Fatalf("last sample %v, want %v", got.LastSampleAt, at(1))
	}
}

func TestMeanCPUUsageUnequalTotals(t *testing.T) {
	for _, tc := range []struct {
		second []float64
		want   float64
	}{{cpuRates(90, 10), 50}, {cpuRates(90, 810), 10}, {cpuRates(900, 0), 91}} {
		values := []Interval[[]float64]{interval(0, 1, cpuRates(10, 90)), interval(1, 2, tc.second)}
		got, err := MeanCPUUsage(Window{at(0), at(2)}, values)
		if err != nil || got.Data == nil {
			t.Fatalf("%+v %v", got, err)
		}
		closeTo(t, got.Data.UserPercent, tc.want)
	}
	// Unequal durations and clipping integrate rates before taking a ratio.
	values := []Interval[[]float64]{
		interval(0, 1, cpuRates(10, 90)),
		interval(1, 3, cpuRates(900, 0)),
	}
	got, err := MeanCPUUsage(Window{at(.5), at(2)}, values)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, got.Data.UserPercent, 905.0/950*100)
}

func TestMeanCPUUsageZeroAndMissing(t *testing.T) {
	zero := interval(0, 1, make([]float64, 8))
	active := interval(1, 2, cpuRates(0, 100))
	active.ObservedAt = at(2)
	for _, values := range [][]Interval[[]float64]{nil, {zero}} {
		got, err := MeanCPUUsage(Window{at(0), at(2)}, values)
		if err != nil || got.Data != nil || got.CoverageRatio != 0 || got.LastSampleAt != nil ||
			got.Partial {
			t.Fatalf("%+v %v", got, err)
		}
	}
	got, err := MeanCPUUsage(Window{at(0), at(3)}, []Interval[[]float64]{zero, active})
	if err != nil || got.Data == nil || !got.Partial || !got.LastSampleAt.Equal(at(2)) {
		t.Fatalf("%+v %v", got, err)
	}
	closeTo(t, got.Data.UserPercent, 0)
	closeTo(t, got.Data.IdlePercent, 100)
	closeTo(t, got.CoverageRatio, 1.0/3)
}

func TestMeanCPUUsageInvalid(t *testing.T) {
	if _, err := MeanCPUUsage(Window{}, nil); err == nil {
		t.Fatal("invalid window")
	}
	for _, values := range [][]Interval[[]float64]{
		{interval(0, 1, []float64{})},
		{interval(0, 1, []float64{1})},
		{interval(0, 1, make([]float64, 9))},
		{interval(2, 1, make([]float64, 8))},
		{interval(0, 2, make([]float64, 8)), interval(1, 3, cpuRates(10, 90))},
		{interval(1, 2, make([]float64, 8)), interval(0, 1, cpuRates(10, 90))},
	} {
		if _, err := MeanCPUUsage(Window{at(0), at(3)}, values); err == nil {
			t.Fatalf("accepted %+v", values)
		}
	}
	for i := 0; i < 8; i++ {
		for _, bad := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
			values := cpuRates(10, 90)
			values[i] = bad
			if _, err := MeanCPUUsage(
				Window{at(0), at(1)},
				[]Interval[[]float64]{interval(0, 1, values)},
			); err == nil {
				t.Fatal("invalid rate")
			}
		}
	}
}

func TestMeanCPUUsageLargeRates(t *testing.T) {
	values := []Interval[[]float64]{
		interval(
			0,
			1,
			[]float64{
				math.MaxFloat64,
				math.MaxFloat64,
				math.MaxFloat64,
				math.MaxFloat64,
				math.MaxFloat64,
				math.MaxFloat64,
				math.MaxFloat64,
				math.MaxFloat64,
			},
		),
	}
	got, err := MeanCPUUsage(Window{at(0), at(1)}, values)
	if err != nil || got.Data == nil {
		t.Fatalf("%+v %v", got, err)
	}
	closeTo(t, got.Data.UserPercent, 25)
	closeTo(t, got.Data.SystemPercent, 37.5)
	closeTo(t, got.Data.IdlePercent, 12.5)
}
