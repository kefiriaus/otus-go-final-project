package aggregate

import (
	"fmt"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

// MeanLoadAverage computes unnormalized time-weighted means of all three
// system indicators with shared coverage, partial status, and observation time.
func MeanLoadAverage(window Window, values []Interval[metrics.LoadAverage]) (Summary[metrics.LoadAverage], error) {
	var projected [3][]Interval[float64]
	for i := range projected {
		projected[i] = make([]Interval[float64], len(values))
	}
	for i, v := range values {
		for j, value := range [3]float64{v.Value.Load1, v.Value.Load5, v.Value.Load15} {
			if value < 0 || !finite(value) {
				return Summary[metrics.LoadAverage]{}, fmt.Errorf("load average component %d must be finite and nonnegative", j)
			}
			projected[j][i] = Interval[float64]{Start: v.Start, End: v.End, ObservedAt: v.ObservedAt, Value: value, Partial: v.Partial}
		}
	}
	var means [3]Summary[float64]
	for i := range means {
		mean, err := WeightedMean(window, projected[i])
		if err != nil {
			return Summary[metrics.LoadAverage]{}, fmt.Errorf("load average component %d: %w", i, err)
		}
		means[i] = mean
	}
	if means[0].Data == nil {
		return Summary[metrics.LoadAverage]{}, nil
	}
	data := metrics.LoadAverage{Load1: *means[0].Data, Load5: *means[1].Data, Load15: *means[2].Data}
	return Summary[metrics.LoadAverage]{Data: &data, CoverageRatio: means[0].CoverageRatio, LastSampleAt: means[0].LastSampleAt, Partial: means[0].Partial}, nil
}
