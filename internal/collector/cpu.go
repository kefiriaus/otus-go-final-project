package collector

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func parseCPUStatLine(data []byte) (metrics.CPUCounters, error) {
	fields := strings.Fields(string(data))
	if len(fields) < 5 || fields[0] != "cpu" {
		return metrics.CPUCounters{}, fmt.Errorf(
			"CPU stat: expected aggregate cpu label and at least four counters",
		)
	}
	var values [8]uint64
	for i, field := range fields[1:] {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return metrics.CPUCounters{}, fmt.Errorf("CPU stat counter %d: %w", i, err)
		}
		if i < len(values) {
			values[i] = value
		}
	}
	return metrics.CPUCounters{
		User:    values[0],
		Nice:    values[1],
		System:  values[2],
		Idle:    values[3],
		IOWait:  values[4],
		IRQ:     values[5],
		SoftIRQ: values[6],
		Steal:   values[7],
	}, nil
}
