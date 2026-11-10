package collector

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func parseLoadAverage(data []byte) (metrics.LoadAverage, error) {
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return metrics.LoadAverage{}, fmt.Errorf("load average: expected at least three fields")
	}
	var values [3]float64
	for i := range values {
		value, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return metrics.LoadAverage{}, fmt.Errorf("load average %s: %w", [3]string{"load1", "load5", "load15"}[i], err)
		}
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return metrics.LoadAverage{}, fmt.Errorf("load average %s: value must be finite and nonnegative", [3]string{"load1", "load5", "load15"}[i])
		}
		values[i] = value
	}
	return metrics.LoadAverage{Load1: values[0], Load5: values[1], Load15: values[2]}, nil
}
