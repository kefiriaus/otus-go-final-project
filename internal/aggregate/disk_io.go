package aggregate

import (
	"fmt"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

// DiskIOCounterPoint preserves all raw components until after subtraction.
// A changed device identity establishes a new baseline in CounterSeries.
func DiskIOCounterPoint(value metrics.DiskIOCounters) CounterPoint {
	return CounterPoint{Generation: value.Identity, Values: []uint64{value.ReadsCompleted, value.WritesCompleted, value.SectorsRead, value.SectorsWritten}}
}

// MeanDiskIO derives completed read+write rates for a single whole device.
// Kernel sectors are always 512 bytes; no hardware sector-size query is needed.
// Unchanged counters represent valid zero activity, not missing data.
func MeanDiskIO(window Window, device string, values []Interval[[]float64]) (Summary[metrics.DiskIO], error) {
	if device == "" {
		return Summary[metrics.DiskIO]{}, fmt.Errorf("disk rate requires a device name")
	}
	for _, v := range values {
		if len(v.Value) != 4 {
			return Summary[metrics.DiskIO]{}, fmt.Errorf("disk rate vector requires four components")
		}
	}
	mean, err := MeanRates(window, values)
	if err != nil {
		return Summary[metrics.DiskIO]{}, fmt.Errorf("disk %s rates: %w", device, err)
	}
	if mean.Data == nil {
		return Summary[metrics.DiskIO]{}, nil
	}
	rates := *mean.Data
	data := metrics.DiskIO{Device: device, TransfersPerSecond: rates[0] + rates[1], KibibytesPerSecond: rates[2]/2 + rates[3]/2}
	if !finite(data.TransfersPerSecond) || !finite(data.KibibytesPerSecond) {
		return Summary[metrics.DiskIO]{}, fmt.Errorf("disk %s derived rate overflow", device)
	}
	return Summary[metrics.DiskIO]{Data: &data, CoverageRatio: mean.CoverageRatio, LastSampleAt: mean.LastSampleAt, Partial: mean.Partial}, nil
}
