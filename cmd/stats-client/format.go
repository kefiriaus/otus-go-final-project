package main

import (
	"fmt"
	"strings"
	"time"

	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func formatCPU(snapshot *statsv1.Snapshot) (string, error) {
	if snapshot == nil {
		return "", fmt.Errorf("nil snapshot")
	}
	if snapshot.Cpu == nil {
		return "", fmt.Errorf("nil CPU section")
	}
	if snapshot.Cpu.Status == nil {
		return "", fmt.Errorf("nil CPU status")
	}
	for _, item := range []struct {
		name      string
		timestamp *timestamppb.Timestamp
	}{
		{"window start", snapshot.WindowStart},
		{"window end", snapshot.WindowEnd},
	} {
		if item.timestamp == nil {
			return "", fmt.Errorf("nil %s timestamp", item.name)
		}
		if err := item.timestamp.CheckValid(); err != nil {
			return "", fmt.Errorf("invalid %s timestamp: %w", item.name, err)
		}
	}
	status := snapshot.Cpu.Status
	if status.LastSampleAt != nil {
		if err := status.LastSampleAt.CheckValid(); err != nil {
			return "", fmt.Errorf("invalid last sample timestamp: %w", err)
		}
	}
	state := strings.TrimPrefix(status.State.String(), "COLLECTION_STATE_")
	var out strings.Builder
	windowStart := snapshot.WindowStart.AsTime().UTC().Format(time.RFC3339Nano)
	windowEnd := snapshot.WindowEnd.AsTime().UTC().Format(time.RFC3339Nano)
	fmt.Fprintf(&out, "window: %s to %s\n", windowStart, windowEnd)
	fmt.Fprintf(&out, "cpu: %s coverage=%.2f%%", state, status.CoverageRatio*100)
	if status.LastSampleAt != nil {
		fmt.Fprintf(
			&out,
			" last_sample=%s",
			status.LastSampleAt.AsTime().UTC().Format(time.RFC3339Nano),
		)
	}
	out.WriteByte('\n')
	if status.Detail != "" {
		fmt.Fprintf(&out, "  detail=%s\n", status.Detail)
	}
	collecting := status.State == statsv1.CollectionState_COLLECTION_STATE_OK ||
		status.State == statsv1.CollectionState_COLLECTION_STATE_PARTIAL
	if snapshot.Cpu.Data != nil && collecting {
		data := snapshot.Cpu.Data
		fmt.Fprintf(
			&out,
			"  user=%.2f%% system=%.2f%% idle=%.2f%% iowait=%.2f%% steal=%.2f%%\n",
			data.UserPercent,
			data.SystemPercent,
			data.IdlePercent,
			data.IowaitPercent,
			data.StealPercent,
		)
	}
	out.WriteByte('\n')
	return out.String(), nil
}
