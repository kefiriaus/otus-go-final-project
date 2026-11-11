package collector

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func parseDiskIOStat(data []byte) (metrics.DiskIOCounters, error) {
	fields := strings.Fields(string(data))
	if len(fields) < 11 {
		return metrics.DiskIOCounters{}, fmt.Errorf("disk stat requires at least eleven counters")
	}
	var selected [4]uint64
	for i, field := range fields {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return metrics.DiskIOCounters{}, fmt.Errorf("disk stat counter %d: %w", i, err)
		}
		switch i {
		case 0:
			selected[0] = value
		case 4:
			selected[1] = value
		case 2:
			selected[2] = value
		case 6:
			selected[3] = value
		}
	}
	return metrics.DiskIOCounters{ReadsCompleted: selected[0], WritesCompleted: selected[1], SectorsRead: selected[2], SectorsWritten: selected[3]}, nil
}

func parseDiskDeviceIdentity(data []byte) (string, error) {
	parts := strings.Split(strings.TrimSpace(string(data)), ":")
	if len(parts) != 2 {
		return "", fmt.Errorf("disk identity requires major:minor")
	}
	var numbers [2]uint64
	for i, part := range parts {
		value, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return "", fmt.Errorf("disk identity component %d: %w", i, err)
		}
		numbers[i] = value
	}
	return fmt.Sprintf("%d:%d", numbers[0], numbers[1]), nil
}
