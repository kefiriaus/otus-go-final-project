package aggregate

import (
	"testing"
	"time"
)

func TestWeightedMeanStaysWithinInputRange(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	firstEnd := start.Add(2 * time.Nanosecond)
	end := start.Add(11 * time.Nanosecond)
	got, err := WeightedMean(
		Window{Start: start, End: end},
		[]Interval[float64]{
			{Start: start, End: firstEnd, ObservedAt: firstEnd, Value: 100},
			{Start: firstEnd, End: end, ObservedAt: end, Value: 100},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.Data == nil {
		t.Fatal("missing mean")
	}
	if *got.Data != 100 {
		t.Fatalf("mean of full values = %.20g, want 100", *got.Data)
	}
}
