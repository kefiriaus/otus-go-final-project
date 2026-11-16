package main

import (
	"strings"
	"testing"

	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/protobuf/proto"
)

func okStatus() *statsv1.CollectionStatus {
	return &statsv1.CollectionStatus{State: statsv1.CollectionState_COLLECTION_STATE_OK, CoverageRatio: 1}
}

func TestFormatLoadAverage(t *testing.T) {
	section := &statsv1.LoadAverageSection{
		Status: okStatus(),
		Data:   &statsv1.LoadAverage{Load1: 1, Load5: 2, Load15: 3},
	}
	got, err := formatLoadAverage(section)
	if err != nil {
		t.Fatal(err)
	}
	want := "load_average: OK coverage=100.00%\n  load1=1.00 load5=2.00 load15=3.00\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatDiskIO(t *testing.T) {
	section := &statsv1.DiskIoSection{
		Status: okStatus(),
		Data: &statsv1.DiskIoData{Devices: []*statsv1.DiskIo{
			{Device: "sda", TransfersPerSecond: 10, KibibytesPerSecond: 20},
			{Device: "nvme0n1"},
		}},
	}
	got, err := formatDiskIO(section)
	if err != nil {
		t.Fatal(err)
	}
	want := "disk_io: OK coverage=100.00%\n" +
		"  device=sda transfers_s=10.00 kib_s=20.00\n" +
		"  device=nvme0n1 transfers_s=0.00 kib_s=0.00\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	section.Data = &statsv1.DiskIoData{}
	got, err = formatDiskIO(section)
	if err != nil {
		t.Fatal(err)
	}
	if got != "disk_io: OK coverage=100.00%\n  devices: empty\n" {
		t.Fatalf("empty output %q", got)
	}
}

func TestFormatFilesystems(t *testing.T) {
	section := &statsv1.FilesystemsSection{
		Status: okStatus(),
		Data: &statsv1.FilesystemsData{Filesystems: []*statsv1.FilesystemUsage{
			{
				MountPoint: "/", Device: "/dev/sda", FilesystemType: "ext4",
				UsedMebibytes: 10, UsedPercent: proto.Float64(50), UsedInodes: proto.Float64(100),
				UsedInodesPercent: proto.Float64(25),
			},
			{
				MountPoint: "/empty", Device: "disk", FilesystemType: "ext4",
				UsedMebibytes: 0,
			},
		}},
	}
	got, err := formatFilesystems(section)
	if err != nil {
		t.Fatal(err)
	}
	want := "filesystems: OK coverage=100.00%\n" +
		"  mount=/ device=/dev/sda type=ext4 used_mib=10.00 used_percent=50.00% used_inodes=100.00" +
		" used_inodes_percent=25.00%\n" +
		"  mount=/empty device=disk type=ext4 used_mib=0.00 used_percent=n/a used_inodes=n/a used_inodes_percent=n/a\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	section.Data = &statsv1.FilesystemsData{}
	got, err = formatFilesystems(section)
	if err != nil {
		t.Fatal(err)
	}
	if got != "filesystems: OK coverage=100.00%\n  filesystems: empty\n" {
		t.Fatalf("empty output %q", got)
	}
}

func TestFormatSnapshotUsesMetricSections(t *testing.T) {
	snapshot := clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_DISABLED)
	snapshot.LoadAverage = &statsv1.LoadAverageSection{
		Status: okStatus(), Data: &statsv1.LoadAverage{Load1: 1, Load5: 2, Load15: 3},
	}
	snapshot.DiskIo = &statsv1.DiskIoSection{Status: okStatus(), Data: &statsv1.DiskIoData{}}
	snapshot.Filesystems = &statsv1.FilesystemsSection{
		Status: okStatus(), Data: &statsv1.FilesystemsData{},
	}
	got, err := formatSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"  load1=1.00 load5=2.00 load15=3.00\n",
		"  devices: empty\n",
		"  filesystems: empty\n",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("snapshot missing %q:\n%s", expected, got)
		}
	}
}
