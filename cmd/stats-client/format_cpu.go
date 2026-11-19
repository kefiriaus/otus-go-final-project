package main

import (
	"fmt"
	"strings"

	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
)

func formatCPU(cpu *statsv1.CpuSection) (string, error) {
	if cpu == nil {
		return "", fmt.Errorf("nil CPU section")
	}
	status, err := formatStatus("cpu", cpu.Status)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	out.WriteString(status)
	collecting := cpu.Status.State == statsv1.CollectionState_COLLECTION_STATE_OK ||
		cpu.Status.State == statsv1.CollectionState_COLLECTION_STATE_PARTIAL
	if cpu.Data != nil && collecting {
		data := cpu.Data
		fmt.Fprintf(
			&out,
			"  user=%.2f%% system=%.2f%% idle=%.2f%% iowait=%.2f%% steal=%.2f%%\n",
			data.UserPercent, data.SystemPercent, data.IdlePercent,
			data.IowaitPercent, data.StealPercent,
		)
	}
	return out.String(), nil
}
