package grpc

import (
	"math"
	"net/netip"
	"testing"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/daemon"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
)

func fixture() daemon.Snapshot {
	at := time.Unix(100, 0).UTC()
	return daemon.Snapshot{Window: aggregate.Window{Start: at, End: at.Add(time.Second)}, GeneratedAt: at.Add(time.Second)}
}
func TestMapStatusesAndPresence(t *testing.T) {
	v := fixture()
	at := v.Window.Start
	zero := 0.0
	v.CPU = daemon.Section[metrics.CPUUsage]{Enabled: true}
	v.LoadAverage = daemon.Section[metrics.LoadAverage]{Enabled: true, Summary: aggregate.Summary[metrics.LoadAverage]{Data: &metrics.LoadAverage{Load1: 3, Load5: 4, Load15: 5}, CoverageRatio: 1, LastSampleAt: &at}}
	fs := []metrics.FilesystemUsage{{MountPoint: "/", Device: "/dev/x", FilesystemType: "ext4", UsedPercent: &zero}}
	v.Filesystems = daemon.Section[[]metrics.FilesystemUsage]{Enabled: true, Summary: aggregate.Summary[[]metrics.FilesystemUsage]{Data: &fs, CoverageRatio: 0.5, LastSampleAt: &at, Partial: true}}
	empty := metrics.Traffic{}
	v.Traffic = daemon.Section[metrics.Traffic]{Enabled: true, Summary: aggregate.Summary[metrics.Traffic]{Data: &empty, CoverageRatio: 1, LastSampleAt: &at, Partial: true}}
	out, err := mapSnapshot(v)
	if err != nil {
		t.Fatal(err)
	}
	if out.LoadAverage.Status.State != statsv1.CollectionState_COLLECTION_STATE_OK || out.LoadAverage.Data.Load15 != 5 {
		t.Fatal(out)
	}
	if out.Cpu.Status.State != statsv1.CollectionState_COLLECTION_STATE_UNAVAILABLE || out.Cpu.Data != nil || out.Cpu.Status.LastSampleAt != nil {
		t.Fatal(out.Cpu)
	}
	if out.DiskIo.Status.State != statsv1.CollectionState_COLLECTION_STATE_DISABLED || out.DiskIo.Data != nil {
		t.Fatal(out.DiskIo)
	}
	if out.Filesystems.Status.State != statsv1.CollectionState_COLLECTION_STATE_PARTIAL || out.Filesystems.Data.Filesystems[0].UsedPercent == nil || out.Filesystems.Data.Filesystems[0].UsedInodes != nil {
		t.Fatal(out.Filesystems)
	}
	*out.Filesystems.Data.Filesystems[0].UsedPercent = 99
	if zero != 0 {
		t.Fatal("optional field aliased")
	}
	if out.Traffic.Data == nil || out.Traffic.Status.State != statsv1.CollectionState_COLLECTION_STATE_PARTIAL {
		t.Fatal(out.Traffic)
	}
}
func TestMapRejectsInconsistentData(t *testing.T) {
	for _, coverage := range []float64{-1, 2, math.NaN()} {
		v := fixture()
		at := v.Window.Start
		v.CPU = daemon.Section[metrics.CPUUsage]{Enabled: true, Summary: aggregate.Summary[metrics.CPUUsage]{Data: &metrics.CPUUsage{}, CoverageRatio: coverage, LastSampleAt: &at}}
		if _, err := mapSnapshot(v); err == nil {
			t.Fatal("invalid coverage accepted")
		}
	}
	v := fixture()
	v.LoadAverage.Summary.Data = &metrics.LoadAverage{}
	if _, err := mapSnapshot(v); err == nil {
		t.Fatal("disabled data accepted")
	}
}

func TestMapAllDataAndCopies(t *testing.T) {
	v := fixture()
	at := v.Window.Start
	zero := 0.0
	pid := uint32(10)
	command := "worker"
	user := "1000"
	v.CPU = daemon.Section[metrics.CPUUsage]{Enabled: true, Summary: aggregate.Summary[metrics.CPUUsage]{Data: &metrics.CPUUsage{UserPercent: 10, SystemPercent: 20, IdlePercent: 60, IOWaitPercent: 5, StealPercent: 5}, CoverageRatio: 1, LastSampleAt: &at}}
	disks := []metrics.DiskIO{{Device: "sda", TransfersPerSecond: 2, KibibytesPerSecond: 3}}
	v.DiskIO = daemon.Section[[]metrics.DiskIO]{Enabled: true, Summary: aggregate.Summary[[]metrics.DiskIO]{Data: &disks, CoverageRatio: 1, LastSampleAt: &at}}
	fs := []metrics.FilesystemUsage{{MountPoint: "/", Device: "sda", FilesystemType: "ext4", UsedMebibytes: 8, UsedPercent: &zero, UsedInodes: &zero, UsedInodesPercent: &zero}}
	v.Filesystems = daemon.Section[[]metrics.FilesystemUsage]{Enabled: true, Summary: aggregate.Summary[[]metrics.FilesystemUsage]{Data: &fs, CoverageRatio: 1, LastSampleAt: &at}}
	source := metrics.Endpoint{Address: netip.MustParseAddr("192.0.2.1"), HasPort: true, Port: 0}
	destination := metrics.Endpoint{Address: netip.MustParseAddr("2001:db8::1")}
	traffic := metrics.Traffic{Protocols: []metrics.ProtocolTraffic{{Protocol: 253, Bytes: 12, Percent: 100}}, Flows: []metrics.FlowTraffic{{Flow: metrics.FlowKey{Source: source, Destination: destination, Protocol: 253}, BytesPerSecond: 4}}}
	v.Traffic = daemon.Section[metrics.Traffic]{Enabled: true, Summary: aggregate.Summary[metrics.Traffic]{Data: &traffic, CoverageRatio: 1, LastSampleAt: &at}}
	connections := metrics.Connections{ListeningSockets: []metrics.ListeningSocket{{Local: source, Protocol: 17, Owners: []metrics.ProcessOwner{{Command: &command, PID: &pid, User: &user}, {}}}}, TCPStates: []metrics.TCPStateCount{{State: "UNKNOWN_FE", Count: 7}}}
	v.Connections = daemon.Section[metrics.Connections]{Enabled: true, Summary: aggregate.Summary[metrics.Connections]{Data: &connections, CoverageRatio: 1, LastSampleAt: &at}}
	out, err := mapSnapshot(v)
	if err != nil {
		t.Fatal(err)
	}
	if out.Cpu.Data.IowaitPercent != 5 || out.DiskIo.Data.Devices[0].KibibytesPerSecond != 3 || out.Filesystems.Data.Filesystems[0].UsedMebibytes != 8 {
		t.Fatal(out)
	}
	flow := out.Traffic.Data.Flows[0]
	if flow.ProtocolNumber != 253 || flow.Source.Port == nil || *flow.Source.Port != 0 || flow.Destination.Port != nil || flow.Destination.Ip != "2001:db8::1" || out.Traffic.Data.Protocols[0].Bytes != 12 {
		t.Fatal(out.Traffic)
	}
	owner := out.Connections.Data.ListeningSockets[0].Owners[0]
	*owner.Command = "changed"
	*owner.Pid = 99
	*owner.User = "changed"
	if command != "worker" || pid != 10 || user != "1000" || out.Connections.Data.TcpStates[0].State != "UNKNOWN_FE" {
		t.Fatal("owner alias or state loss")
	}
}
