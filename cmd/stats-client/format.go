package main

import (
	"fmt"
	"strings"
	"time"

	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const notAvailable = "n/a"

type namedStatus struct {
	name   string
	status *statsv1.CollectionStatus
}

func formatStatus(name string, status *statsv1.CollectionStatus) (string, error) {
	if status == nil {
		return "", fmt.Errorf("nil %s status", name)
	}
	if status.LastSampleAt != nil {
		if err := status.LastSampleAt.CheckValid(); err != nil {
			return "", fmt.Errorf("invalid %s last sample timestamp: %w", name, err)
		}
	}
	state := strings.TrimPrefix(status.State.String(), "COLLECTION_STATE_")
	var out strings.Builder
	fmt.Fprintf(&out, "%s: %s coverage=%.2f%%", name, state, status.CoverageRatio*100)
	if status.LastSampleAt != nil {
		last := status.LastSampleAt.AsTime().UTC().Format(time.RFC3339Nano)
		fmt.Fprintf(&out, " last_sample=%s", last)
	}
	out.WriteByte('\n')
	if status.Detail != "" {
		fmt.Fprintf(&out, "  detail=%s\n", status.Detail)
	}
	return out.String(), nil
}

func snapshotSections(snapshot *statsv1.Snapshot) ([]namedStatus, error) {
	if snapshot.LoadAverage == nil || snapshot.Cpu == nil || snapshot.DiskIo == nil ||
		snapshot.Filesystems == nil || snapshot.Traffic == nil || snapshot.Connections == nil {
		return nil, fmt.Errorf("snapshot requires all six sections")
	}
	return []namedStatus{
		{"load_average", snapshot.LoadAverage.Status},
		{"cpu", snapshot.Cpu.Status},
		{"disk_io", snapshot.DiskIo.Status},
		{"filesystems", snapshot.Filesystems.Status},
		{"traffic", snapshot.Traffic.Status},
		{"connections", snapshot.Connections.Status},
	}, nil
}

func formatSnapshot(snapshot *statsv1.Snapshot) (string, error) {
	if snapshot == nil {
		return "", fmt.Errorf("nil snapshot")
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
	sections, err := snapshotSections(snapshot)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	windowStart := snapshot.WindowStart.AsTime().UTC().Format(time.RFC3339Nano)
	windowEnd := snapshot.WindowEnd.AsTime().UTC().Format(time.RFC3339Nano)
	fmt.Fprintf(&out, "window: %s to %s\n", windowStart, windowEnd)
	for _, section := range sections {
		var text string
		var err error
		switch section.name {
		case "load_average":
			text, err = formatLoadAverage(snapshot.LoadAverage)
		case "cpu":
			text, err = formatCPU(snapshot.Cpu)
		case "disk_io":
			text, err = formatDiskIO(snapshot.DiskIo)
		case "filesystems":
			text, err = formatFilesystems(snapshot.Filesystems)
		case "traffic":
			text, err = formatTraffic(snapshot.Traffic)
		case "connections":
			text, err = formatConnections(snapshot.Connections)
		default:
			text, err = formatStatus(section.name, section.status)
		}
		if err != nil {
			return "", err
		}
		out.WriteString(text)
	}
	out.WriteByte('\n')
	return out.String(), nil
}
