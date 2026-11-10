package aggregate

import (
	"math"
	"testing"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func TestMeanLoadAverage(t *testing.T) {
	values := []Interval[metrics.LoadAverage]{interval(0, 1, metrics.LoadAverage{Load1: 10, Load5: 20, Load15: 30}), interval(1, 3, metrics.LoadAverage{Load1: 20, Load5: 40, Load15: 60})}
	got, err := MeanLoadAverage(Window{at(0), at(4)}, values)
	if err != nil || got.Data == nil || !got.Partial || got.CoverageRatio != .75 || !got.LastSampleAt.Equal(at(1)) {
		t.Fatalf("%+v %v", got, err)
	}
	closeTo(t, got.Data.Load1, 50.0/3)
	closeTo(t, got.Data.Load5, 100.0/3)
	closeTo(t, got.Data.Load15, 50)
	clipped, err := MeanLoadAverage(Window{at(.5), at(1.5)}, values)
	if err != nil || clipped.Data == nil || clipped.Partial || clipped.CoverageRatio != 1 {
		t.Fatalf("%+v %v", clipped, err)
	}
	if *clipped.Data != (metrics.LoadAverage{Load1: 15, Load5: 30, Load15: 45}) {
		t.Fatal(*clipped.Data)
	}
	empty, err := MeanLoadAverage(Window{at(10), at(11)}, values)
	if err != nil || empty.Data != nil || empty.LastSampleAt != nil || empty.CoverageRatio != 0 || empty.Partial {
		t.Fatalf("%+v %v", empty, err)
	}
	empty, err = MeanLoadAverage(Window{at(0), at(1)}, nil)
	if err != nil || empty.Data != nil {
		t.Fatalf("%+v %v", empty, err)
	}
	zero := interval(0, 1, metrics.LoadAverage{})
	zero.Partial = true
	measured, err := MeanLoadAverage(Window{at(0), at(1)}, []Interval[metrics.LoadAverage]{zero})
	if err != nil || measured.Data == nil || *measured.Data != (metrics.LoadAverage{}) || measured.CoverageRatio != 1 || !measured.Partial {
		t.Fatalf("%+v %v", measured, err)
	}
}
func TestMeanLoadAverageInvalid(t *testing.T) {
	for _, component := range []int{0, 1, 2} {
		for _, bad := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
			value := metrics.LoadAverage{Load1: 1, Load5: 2, Load15: 3}
			switch component {
			case 0:
				value.Load1 = bad
			case 1:
				value.Load5 = bad
			case 2:
				value.Load15 = bad
			}
			got, err := MeanLoadAverage(Window{at(0), at(1)}, []Interval[metrics.LoadAverage]{interval(0, 1, value)})
			if err == nil || got.Data != nil {
				t.Fatalf("accepted %+v", value)
			}
		}
	}
	if _, err := MeanLoadAverage(Window{}, nil); err == nil {
		t.Fatal("invalid window")
	}
	for _, values := range [][]Interval[metrics.LoadAverage]{{interval(1, 0, metrics.LoadAverage{})}, {interval(0, 2, metrics.LoadAverage{}), interval(1, 3, metrics.LoadAverage{})}} {
		if _, err := MeanLoadAverage(Window{at(0), at(4)}, values); err == nil {
			t.Fatal("invalid intervals")
		}
	}
}
