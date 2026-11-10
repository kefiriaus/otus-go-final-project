package aggregate

import (
	"errors"
	"reflect"
	"testing"
)

func TestSharedHistoryClientWindows(t *testing.T) {
	gauge, _ := NewGaugeSeries[float64](4)
	counter, _ := NewCounterSeries(4)
	gaugeEvents := []Event[float64]{{At: at(0), Value: 10}, {At: at(1), Value: 20}, {At: at(2), Err: errors.New("unavailable")}, {At: at(3), Value: 40}, {At: at(4), Value: 50}}
	for _, event := range gaugeEvents {
		if err := gauge.Add(event); err != nil {
			t.Fatal(err)
		}
	}
	for _, event := range []Event[CounterPoint]{{At: at(0), Value: CounterPoint{Values: []uint64{100}}}, {At: at(1), Value: CounterPoint{Values: []uint64{160}}}, {At: at(2), Err: errors.New("unavailable")}, {At: at(3), Value: CounterPoint{Values: []uint64{300}}}, {At: at(4), Value: CounterPoint{Values: []uint64{330}}}} {
		if err := counter.Add(event); err != nil {
			t.Fatal(err)
		}
	}
	early := Window{at(0), at(4)}
	late := Window{at(1), at(5)}
	g1, _ := gauge.Read(early)
	g2, _ := gauge.Read(late)
	a, err := WeightedMean(early, g1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := WeightedMean(late, g2)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, *a.Data, 70.0/3)
	closeTo(t, *b.Data, 110.0/3)
	closeTo(t, a.CoverageRatio, .75)
	closeTo(t, b.CoverageRatio, .75)
	c1, _ := counter.Read(early)
	c2, _ := counter.Read(late)
	ca, _ := MeanRates(early, c1)
	cb, _ := MeanRates(late, c2)
	closeTo(t, (*ca.Data)[0], 45)
	closeTo(t, ca.CoverageRatio, .5)
	closeTo(t, (*cb.Data)[0], 30)
	closeTo(t, cb.CoverageRatio, .25)
	again, _ := gauge.Read(early)
	if !reflect.DeepEqual(g1, again) {
		t.Fatal("another client consumed history")
	}
	// A long gap must replace the baseline rather than create a long rate interval.
	_ = counter.Add(Event[CounterPoint]{At: at(100), Value: CounterPoint{Values: []uint64{1000}}})
	_ = counter.Add(Event[CounterPoint]{At: at(101), Value: CounterPoint{Values: []uint64{1010}}})
	recovered, _ := counter.Read(Window{at(95), at(102)})
	if len(recovered) != 1 || !recovered[0].Start.Equal(at(100)) || recovered[0].Value[0] != 10 {
		t.Fatal(recovered)
	}
}
