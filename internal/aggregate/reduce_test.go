package aggregate

import (
	"math"
	"reflect"
	"testing"
)

func closeTo(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("got %v, want %v", got, want)
	}
}
func TestWeightedMean(t *testing.T) {
	values := []Interval[float64]{interval(0, 1, 10.0), interval(1, 3, 20.0)}
	got, err := WeightedMean(Window{at(0), at(4)}, values)
	if err != nil || got.Data == nil || !got.Partial || !got.LastSampleAt.Equal(at(1)) {
		t.Fatalf("%+v %v", got, err)
	}
	closeTo(t, *got.Data, 50.0/3)
	closeTo(t, got.CoverageRatio, .75)
	clipped, err := WeightedMean(Window{at(.5), at(1.5)}, values)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, *clipped.Data, 15)
	if clipped.Partial {
		t.Fatal(clipped)
	}
	zero, _ := WeightedMean(Window{at(0), at(1)}, []Interval[float64]{interval(0, 1, 0.0)})
	if zero.Data == nil || *zero.Data != 0 || zero.CoverageRatio != 1 || zero.Partial {
		t.Fatal(zero)
	}
	partial := interval(0, 1, 0.0)
	partial.Partial = true
	p, _ := WeightedMean(Window{at(0), at(1)}, []Interval[float64]{partial})
	if !p.Partial || p.CoverageRatio != 1 {
		t.Fatal(p)
	}
	empty, _ := WeightedMean(Window{at(5), at(6)}, values)
	if empty.Data != nil || empty.LastSampleAt != nil || empty.CoverageRatio != 0 || empty.Partial {
		t.Fatal(empty)
	}
	if !reflect.DeepEqual(values, []Interval[float64]{interval(0, 1, 10.0), interval(1, 3, 20.0)}) {
		t.Fatal("mutated input")
	}
}
func TestWeightedMeanValidation(t *testing.T) {
	w := Window{at(0), at(4)}
	for _, v := range [][]Interval[float64]{{interval(0, 1, math.NaN())}, {interval(0, 1, math.Inf(1))}, {interval(0, 2, 1.0), interval(1, 3, 2.0)}, {interval(1, 2, 1.0), interval(0, 1, 2.0)}, {interval(2, 1, 1.0)}} {
		if _, err := WeightedMean(w, v); err == nil {
			t.Fatalf("accepted %+v", v)
		}
	}
	if _, err := WeightedMean(Window{}, nil); err == nil {
		t.Fatal("invalid window")
	}
}
func TestMeanRates(t *testing.T) {
	first := interval(0, 1, []float64{10, 0})
	second := interval(1, 3, []float64{20, 30})
	second.ObservedAt = at(3)
	got, err := MeanRates(Window{at(0), at(3)}, []Interval[[]float64]{first, second})
	if err != nil || got.Data == nil || !got.LastSampleAt.Equal(at(3)) {
		t.Fatalf("%+v %v", got, err)
	}
	closeTo(t, (*got.Data)[0], 50.0/3)
	closeTo(t, (*got.Data)[1], 20)
	(*got.Data)[0] = 999
	if first.Value[0] != 10 {
		t.Fatal("aliased input")
	}
	for _, v := range [][]Interval[[]float64]{{interval(0, 1, []float64{})}, {interval(0, 1, []float64{-1})}, {interval(0, 1, []float64{math.Inf(1)})}, {first, interval(1, 2, []float64{1})}} {
		if _, err := MeanRates(Window{at(0), at(3)}, v); err == nil {
			t.Fatal("invalid rates")
		}
	}
	empty, err := MeanRates(Window{at(0), at(3)}, nil)
	if err != nil || empty.Data != nil {
		t.Fatalf("%+v %v", empty, err)
	}
}
func TestLatest(t *testing.T) {
	first := interval(0, 1, "old")
	second := interval(1, 2, "new")
	excluded := interval(2, 3, "at-end")
	excluded.ObservedAt = at(3)
	values := []Interval[string]{first, second, excluded}
	got, err := Latest(Window{at(.5), at(3)}, values)
	if err != nil || got.Data == nil || *got.Data != "new" || !got.LastSampleAt.Equal(at(1)) || got.CoverageRatio != 1 {
		t.Fatalf("%+v %v", got, err)
	}
	empty, err := Latest(Window{at(2), at(3)}, []Interval[string]{excluded})
	if err != nil || empty.Data != nil || empty.LastSampleAt != nil || empty.CoverageRatio != 0 {
		t.Fatalf("%+v %v", empty, err)
	}
	if _, err := Latest(Window{}, values); err == nil {
		t.Fatal("invalid window")
	}
}
