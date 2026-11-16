package main

import (
	"testing"
	"time"

	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func disabledStatus() *statsv1.CollectionStatus {
	return &statsv1.CollectionStatus{State: statsv1.CollectionState_COLLECTION_STATE_DISABLED}
}

func clientSnapshot(state statsv1.CollectionState) *statsv1.Snapshot {
	start := time.Date(2026, 11, 12, 21, 0, 0, 0, time.UTC)
	end := time.Date(2026, 11, 12, 21, 0, 15, 0, time.UTC)
	last := time.Date(2026, 11, 12, 21, 0, 14, 0, time.UTC)
	snapshot := &statsv1.Snapshot{
		WindowStart: timestamppb.New(start),
		WindowEnd:   timestamppb.New(end),
		LoadAverage: &statsv1.LoadAverageSection{Status: disabledStatus()},
		Cpu: &statsv1.CpuSection{Status: &statsv1.CollectionStatus{
			State: state, CoverageRatio: 1, LastSampleAt: timestamppb.New(last),
		}},
		DiskIo:      &statsv1.DiskIoSection{Status: disabledStatus()},
		Filesystems: &statsv1.FilesystemsSection{Status: disabledStatus()},
		Traffic:     &statsv1.TrafficSection{Status: disabledStatus()},
		Connections: &statsv1.ConnectionsSection{Status: disabledStatus()},
	}
	if state == statsv1.CollectionState_COLLECTION_STATE_OK ||
		state == statsv1.CollectionState_COLLECTION_STATE_PARTIAL {
		snapshot.Cpu.Data = &statsv1.CpuUsage{
			UserPercent: 12.5, SystemPercent: 5.25, IdlePercent: 80,
			IowaitPercent: 1.25, StealPercent: 1,
		}
	}
	return snapshot
}

func expectedSnapshot(cpu string) string {
	return "window: 2026-11-12T21:00:00Z to 2026-11-12T21:00:15Z\n" +
		"load_average: DISABLED coverage=0.00%\n" + cpu +
		"disk_io: DISABLED coverage=0.00%\n" +
		"filesystems: DISABLED coverage=0.00%\n" +
		"traffic: DISABLED coverage=0.00%\n" +
		"connections: DISABLED coverage=0.00%\n\n"
}

func TestFormatSnapshotCPUStates(t *testing.T) {
	ok := clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_OK)
	partial := clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_PARTIAL)
	partial.Cpu.Status.CoverageRatio = 0.5
	partial.Cpu.Status.Detail = "cpu degraded"
	unavailable := clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_UNAVAILABLE)
	unavailable.Cpu.Status.CoverageRatio = 0
	unavailable.Cpu.Status.LastSampleAt = nil
	unavailable.Cpu.Status.Detail = "source unavailable"
	unavailable.Cpu.Data = &statsv1.CpuUsage{UserPercent: 100}
	disabled := clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_DISABLED)
	disabled.Cpu.Status.CoverageRatio = 0
	disabled.Cpu.Status.LastSampleAt = nil
	disabled.Cpu.Data = &statsv1.CpuUsage{IdlePercent: 100}
	zero := clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_OK)
	zero.Cpu.Data = &statsv1.CpuUsage{}
	for _, tt := range []struct {
		name     string
		snapshot *statsv1.Snapshot
		want     string
	}{
		{
			"ok", ok,
			expectedSnapshot("cpu: OK coverage=100.00% last_sample=2026-11-12T21:00:14Z\n" +
				"  user=12.50% system=5.25% idle=80.00% iowait=1.25% steal=1.00%\n"),
		},
		{
			"partial", partial,
			expectedSnapshot("cpu: PARTIAL coverage=50.00% last_sample=2026-11-12T21:00:14Z\n" +
				"  detail=cpu degraded\n" +
				"  user=12.50% system=5.25% idle=80.00% iowait=1.25% steal=1.00%\n"),
		},
		{"unavailable", unavailable, expectedSnapshot("cpu: UNAVAILABLE coverage=0.00%\n  detail=source unavailable\n")},
		{"disabled", disabled, expectedSnapshot("cpu: DISABLED coverage=0.00%\n")},
		{
			"measured zero", zero,
			expectedSnapshot("cpu: OK coverage=100.00% last_sample=2026-11-12T21:00:14Z\n" +
				"  user=0.00% system=0.00% idle=0.00% iowait=0.00% steal=0.00%\n"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := formatSnapshot(tt.snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got:\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}

func TestFormatSnapshotRejectsNilEntries(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*statsv1.Snapshot)
	}{
		{"disk device", func(s *statsv1.Snapshot) {
			s.DiskIo = &statsv1.DiskIoSection{Status: okStatus(), Data: &statsv1.DiskIoData{Devices: []*statsv1.DiskIo{nil}}}
		}},
		{"filesystem", func(s *statsv1.Snapshot) {
			s.Filesystems = &statsv1.FilesystemsSection{
				Status: okStatus(), Data: &statsv1.FilesystemsData{Filesystems: []*statsv1.FilesystemUsage{nil}},
			}
		}},
		{"traffic protocol", func(s *statsv1.Snapshot) {
			s.Traffic = &statsv1.TrafficSection{
				Status: okStatus(), Data: &statsv1.TrafficData{Protocols: []*statsv1.ProtocolTraffic{nil}},
			}
		}},
		{"traffic flow", func(s *statsv1.Snapshot) {
			s.Traffic = &statsv1.TrafficSection{
				Status: okStatus(), Data: &statsv1.TrafficData{Flows: []*statsv1.FlowTraffic{nil}},
			}
		}},
		{"listening socket", func(s *statsv1.Snapshot) {
			s.Connections = &statsv1.ConnectionsSection{
				Status: okStatus(), Data: &statsv1.ConnectionsData{ListeningSockets: []*statsv1.ListeningSocket{nil}},
			}
		}},
		{"TCP state", func(s *statsv1.Snapshot) {
			s.Connections = &statsv1.ConnectionsSection{
				Status: okStatus(), Data: &statsv1.ConnectionsData{TcpStates: []*statsv1.TcpStateCount{nil}},
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("panic: %v", recovered)
				}
			}()
			snapshot := clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_DISABLED)
			tt.mutate(snapshot)
			got, err := formatSnapshot(snapshot)
			if err == nil || got != "" {
				t.Fatalf("got %q, %v; want empty output and error", got, err)
			}
		})
	}
}

func TestFormatSnapshotRejectsMalformedSnapshots(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*statsv1.Snapshot)
	}{
		{"nil load average", func(s *statsv1.Snapshot) { s.LoadAverage = nil }},
		{"nil cpu", func(s *statsv1.Snapshot) { s.Cpu = nil }},
		{"nil disk", func(s *statsv1.Snapshot) { s.DiskIo = nil }},
		{"nil filesystems", func(s *statsv1.Snapshot) { s.Filesystems = nil }},
		{"nil traffic", func(s *statsv1.Snapshot) { s.Traffic = nil }},
		{"nil connections", func(s *statsv1.Snapshot) { s.Connections = nil }},
		{"nil load status", func(s *statsv1.Snapshot) { s.LoadAverage.Status = nil }},
		{"nil cpu status", func(s *statsv1.Snapshot) { s.Cpu.Status = nil }},
		{"nil disk status", func(s *statsv1.Snapshot) { s.DiskIo.Status = nil }},
		{"nil filesystems status", func(s *statsv1.Snapshot) { s.Filesystems.Status = nil }},
		{"nil traffic status", func(s *statsv1.Snapshot) { s.Traffic.Status = nil }},
		{"nil connections status", func(s *statsv1.Snapshot) { s.Connections.Status = nil }},
		{"nil window start", func(s *statsv1.Snapshot) { s.WindowStart = nil }},
		{"nil window end", func(s *statsv1.Snapshot) { s.WindowEnd = nil }},
		{"invalid window start", func(s *statsv1.Snapshot) {
			s.WindowStart = &timestamppb.Timestamp{Seconds: 253402300800}
		}},
		{"invalid window end", func(s *statsv1.Snapshot) {
			s.WindowEnd = &timestamppb.Timestamp{Seconds: 253402300800}
		}},
		{"invalid last sample", func(s *statsv1.Snapshot) {
			s.Connections.Status.LastSampleAt = &timestamppb.Timestamp{Seconds: 253402300800}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_OK)
			tt.mutate(snapshot)
			if _, err := formatSnapshot(snapshot); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := formatSnapshot(nil); err == nil {
		t.Fatal("expected nil snapshot error")
	}
}
