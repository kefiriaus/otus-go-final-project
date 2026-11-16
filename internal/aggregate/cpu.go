package aggregate

import (
	"fmt"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

// CPUCounterPoint preserves each raw accounting category until after subtraction.
// Generation is process-local: a daemon restart establishes a fresh baseline.
func CPUCounterPoint(value metrics.CPUCounters) CounterPoint {
	return CounterPoint{Values: []uint64{
		value.User, value.Nice, value.System, value.Idle,
		value.IOWait, value.IRQ, value.SoftIRQ, value.Steal,
	}}
}

func cpuRateScale(values []Interval[[]float64], selection selection) (float64, error) {
	scale := 0.0
	for i, v := range values {
		if len(v.Value) != 8 {
			return 0, fmt.Errorf("CPU rate vector requires eight categories")
		}
		for _, rate := range v.Value {
			if !finite(rate) || rate < 0 {
				return 0, fmt.Errorf("CPU rates must be finite and nonnegative")
			}
			if selection.durations[i] > 0 && rate > scale {
				scale = rate
			}
		}
	}
	return scale, nil
}

func scaledCPUIntervals(values []Interval[[]float64], selection selection, scale float64) []Interval[[]float64] {
	usable := make([]Interval[[]float64], 0, len(values))
	for i, v := range values {
		if selection.durations[i] == 0 {
			continue
		}
		positive := false
		for _, rate := range v.Value {
			positive = positive || rate > 0
		}
		if !positive {
			continue
		}
		rates := make([]float64, 8)
		for j, rate := range v.Value {
			rates[j] = rate / scale
		}
		v.Value = rates
		usable = append(usable, v)
	}
	return usable
}

// MeanCPUUsage derives a CPU distribution from accumulated usable category rates.
// All-zero intervals have no usable distribution and do not contribute coverage.
// Guest categories are not included again; user/nice already account for them.
func MeanCPUUsage(window Window, values []Interval[[]float64]) (Summary[metrics.CPUUsage], error) {
	selection, err := selectIntervals(window, values)
	if err != nil {
		return Summary[metrics.CPUUsage]{}, err
	}
	scale, err := cpuRateScale(values, selection)
	if err != nil || scale == 0 {
		return Summary[metrics.CPUUsage]{}, err
	}
	usable := scaledCPUIntervals(values, selection, scale)
	mean, err := MeanRates(window, usable)
	if err != nil {
		return Summary[metrics.CPUUsage]{}, fmt.Errorf("CPU mean rates: %w", err)
	}
	rates := *mean.Data
	total := 0.0
	for _, rate := range rates {
		total += rate
	}
	if total <= 0 || !finite(total) {
		return Summary[metrics.CPUUsage]{}, fmt.Errorf("CPU total rate must be positive and finite")
	}
	percentages := [5]float64{
		(rates[0] + rates[1]) / total * 100,
		(rates[2] + rates[5] + rates[6]) / total * 100,
		rates[3] / total * 100,
		rates[4] / total * 100,
		rates[7] / total * 100,
	}
	for i, value := range percentages {
		if !finite(value) || value < -1e-9 || value > 100+1e-9 {
			return Summary[metrics.CPUUsage]{}, fmt.Errorf("CPU percentage outside 0..100")
		}
		percentages[i] = min(100, max(0, value))
	}
	data := metrics.CPUUsage{
		UserPercent: percentages[0], SystemPercent: percentages[1],
		IdlePercent: percentages[2], IOWaitPercent: percentages[3], StealPercent: percentages[4],
	}
	return Summary[metrics.CPUUsage]{
		Data: &data, CoverageRatio: mean.CoverageRatio,
		LastSampleAt: mean.LastSampleAt, Partial: mean.Partial,
	}, nil
}
