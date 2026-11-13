package main

import (
	"testing"
	"time"

	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func cpuFixture(state statsv1.CollectionState) *statsv1.Snapshot {
	start := time.Date(2026, 11, 12, 21, 0, 0, 0, time.UTC)
	end := time.Date(2026, 11, 12, 21, 0, 15, 0, time.UTC)
	last := time.Date(2026, 11, 12, 21, 0, 14, 0, time.UTC)
	snapshot := &statsv1.Snapshot{
		WindowStart: timestamppb.New(start),
		WindowEnd:   timestamppb.New(end),
		Cpu: &statsv1.CpuSection{Status: &statsv1.CollectionStatus{
			State:         state,
			CoverageRatio: 1,
			LastSampleAt:  timestamppb.New(last),
		}},
	}
	if state == statsv1.CollectionState_COLLECTION_STATE_OK || state == statsv1.CollectionState_COLLECTION_STATE_PARTIAL {
		snapshot.Cpu.Data = &statsv1.CpuUsage{UserPercent: 12.5, SystemPercent: 5.25, IdlePercent: 80, IowaitPercent: 1.25, StealPercent: 1}
	}
	return snapshot
}

func TestFormatCPURejectsMalformedSnapshots(t *testing.T) {
	nilCPU := cpuFixture(statsv1.CollectionState_COLLECTION_STATE_OK)
	nilCPU.Cpu = nil
	nilStatus := cpuFixture(statsv1.CollectionState_COLLECTION_STATE_OK)
	nilStatus.Cpu.Status = nil
	nilStart := cpuFixture(statsv1.CollectionState_COLLECTION_STATE_OK)
	nilStart.WindowStart = nil
	nilEnd := cpuFixture(statsv1.CollectionState_COLLECTION_STATE_OK)
	nilEnd.WindowEnd = nil
	invalidStart := cpuFixture(statsv1.CollectionState_COLLECTION_STATE_OK)
	invalidStart.WindowStart = &timestamppb.Timestamp{Seconds: 253402300800}
	invalidEnd := cpuFixture(statsv1.CollectionState_COLLECTION_STATE_OK)
	invalidEnd.WindowEnd = &timestamppb.Timestamp{Seconds: 253402300800}
	invalidLast := cpuFixture(statsv1.CollectionState_COLLECTION_STATE_OK)
	invalidLast.Cpu.Status.LastSampleAt = &timestamppb.Timestamp{Seconds: 253402300800}
	for _, tt := range []struct {
		name     string
		snapshot *statsv1.Snapshot
	}{
		{"nil snapshot", nil},
		{"nil cpu", nilCPU},
		{"nil status", nilStatus},
		{"nil window start", nilStart},
		{"nil window end", nilEnd},
		{"invalid window start", invalidStart},
		{"invalid window end", invalidEnd},
		{"invalid last sample", invalidLast},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := formatCPU(tt.snapshot); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestFormatCPUStates(t *testing.T) {
	ok := cpuFixture(statsv1.CollectionState_COLLECTION_STATE_OK)
	partial := cpuFixture(statsv1.CollectionState_COLLECTION_STATE_PARTIAL)
	partial.Cpu.Status.CoverageRatio = 0.5
	partial.Cpu.Status.Detail = "cpu degraded"
	unavailable := cpuFixture(statsv1.CollectionState_COLLECTION_STATE_UNAVAILABLE)
	unavailable.Cpu.Status.CoverageRatio = 0
	unavailable.Cpu.Status.LastSampleAt = nil
	unavailable.Cpu.Status.Detail = "source unavailable"
	unavailable.Cpu.Data = &statsv1.CpuUsage{UserPercent: 100}
	disabled := cpuFixture(statsv1.CollectionState_COLLECTION_STATE_DISABLED)
	disabled.Cpu.Status.CoverageRatio = 0
	disabled.Cpu.Status.LastSampleAt = nil
	disabled.Cpu.Data = &statsv1.CpuUsage{IdlePercent: 100}
	zero := cpuFixture(statsv1.CollectionState_COLLECTION_STATE_OK)
	zero.Cpu.Data = &statsv1.CpuUsage{}
	for _, tt := range []struct {
		name     string
		snapshot *statsv1.Snapshot
		want     string
	}{
		{"ok", ok, "window: 2026-11-12T21:00:00Z to 2026-11-12T21:00:15Z\ncpu: OK coverage=100.00% last_sample=2026-11-12T21:00:14Z\n  user=12.50% system=5.25% idle=80.00% iowait=1.25% steal=1.00%\n\n"},
		{"partial", partial, "window: 2026-11-12T21:00:00Z to 2026-11-12T21:00:15Z\ncpu: PARTIAL coverage=50.00% last_sample=2026-11-12T21:00:14Z\n  detail=cpu degraded\n  user=12.50% system=5.25% idle=80.00% iowait=1.25% steal=1.00%\n\n"},
		{"unavailable", unavailable, "window: 2026-11-12T21:00:00Z to 2026-11-12T21:00:15Z\ncpu: UNAVAILABLE coverage=0.00%\n  detail=source unavailable\n\n"},
		{"disabled", disabled, "window: 2026-11-12T21:00:00Z to 2026-11-12T21:00:15Z\ncpu: DISABLED coverage=0.00%\n\n"},
		{"measured zero", zero, "window: 2026-11-12T21:00:00Z to 2026-11-12T21:00:15Z\ncpu: OK coverage=100.00% last_sample=2026-11-12T21:00:14Z\n  user=0.00% system=0.00% idle=0.00% iowait=0.00% steal=0.00%\n\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := formatCPU(tt.snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got:\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}
