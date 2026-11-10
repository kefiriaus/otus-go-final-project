package statsv1_test

import (
	"testing"
	"time"

	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func roundTrip(t *testing.T, want *statsv1.Snapshot) *statsv1.Snapshot {
	t.Helper()
	wire, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got := new(statsv1.Snapshot)
	if err := proto.Unmarshal(wire, got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(want, got) {
		t.Fatalf("round trip changed message:\nwant %v\ngot %v", want, got)
	}
	return got
}

func TestSnapshotPresenceRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		snapshot *statsv1.Snapshot
		check    func(*testing.T, *statsv1.Snapshot)
	}{
		{"measured CPU zero", &statsv1.Snapshot{Cpu: &statsv1.CpuSection{Status: &statsv1.CollectionStatus{State: statsv1.CollectionState_COLLECTION_STATE_OK, CoverageRatio: 1}, Data: &statsv1.CpuUsage{IdlePercent: 0}}}, func(t *testing.T, got *statsv1.Snapshot) {
			if got.Cpu.Data == nil || got.Cpu.Data.IdlePercent != 0 {
				t.Fatal("measured zero lost")
			}
		}},
		{"unavailable CPU", &statsv1.Snapshot{Cpu: &statsv1.CpuSection{Status: &statsv1.CollectionStatus{State: statsv1.CollectionState_COLLECTION_STATE_UNAVAILABLE}}}, func(t *testing.T, got *statsv1.Snapshot) {
			if got.Cpu.Data != nil {
				t.Fatal("unavailable CPU gained data")
			}
		}},
		{"empty successful traffic", &statsv1.Snapshot{Traffic: &statsv1.TrafficSection{Status: &statsv1.CollectionStatus{State: statsv1.CollectionState_COLLECTION_STATE_OK, CoverageRatio: 1}, Data: &statsv1.TrafficData{}}}, func(t *testing.T, got *statsv1.Snapshot) {
			if got.Traffic.Data == nil || len(got.Traffic.Data.Protocols) != 0 || len(got.Traffic.Data.Flows) != 0 {
				t.Fatal("empty successful traffic changed")
			}
		}},
		{"disabled traffic", &statsv1.Snapshot{Traffic: &statsv1.TrafficSection{Status: &statsv1.CollectionStatus{State: statsv1.CollectionState_COLLECTION_STATE_DISABLED}}}, func(t *testing.T, got *statsv1.Snapshot) {
			if got.Traffic.Data != nil {
				t.Fatal("disabled traffic gained data")
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { tt.check(t, roundTrip(t, tt.snapshot)) })
	}
}

func TestEndpointPortPresenceRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		name     string
		protocol uint32
		port     *uint32
	}{
		{"ICMP without port", 1, nil}, {"TCP explicit zero", 6, proto.Uint32(0)}, {"UDP maximum port", 17, proto.Uint32(65535)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := roundTrip(t, &statsv1.Snapshot{Traffic: &statsv1.TrafficSection{Data: &statsv1.TrafficData{Flows: []*statsv1.FlowTraffic{{Source: &statsv1.Endpoint{Ip: "192.0.2.1", Port: tt.port}, Destination: &statsv1.Endpoint{Ip: "2001:db8::1", Port: tt.port}, ProtocolNumber: tt.protocol}}}}})
			flow := got.Traffic.Data.Flows[0]
			for _, endpoint := range []*statsv1.Endpoint{flow.Source, flow.Destination} {
				if (endpoint.Port == nil) != (tt.port == nil) {
					t.Fatal("port presence changed")
				}
				if tt.port != nil && *endpoint.Port != *tt.port {
					t.Fatal("port value changed")
				}
			}
		})
	}
}

func TestFilesystemPresenceRoundTrip(t *testing.T) {
	got := roundTrip(t, &statsv1.Snapshot{Filesystems: &statsv1.FilesystemsSection{Data: &statsv1.FilesystemsData{Filesystems: []*statsv1.FilesystemUsage{
		{MountPoint: "/unknown"},
		{MountPoint: "/empty", UsedPercent: proto.Float64(0), UsedInodes: proto.Float64(0), UsedInodesPercent: proto.Float64(0)},
	}}}})
	unknown, zero := got.Filesystems.Data.Filesystems[0], got.Filesystems.Data.Filesystems[1]
	if unknown.UsedPercent != nil || unknown.UsedInodes != nil || unknown.UsedInodesPercent != nil {
		t.Fatal("unknown filesystem values became measured")
	}
	if zero.UsedPercent == nil || zero.UsedInodes == nil || zero.UsedInodesPercent == nil {
		t.Fatal("measured filesystem zeros lost presence")
	}
}

func TestConnectionsRoundTrip(t *testing.T) {
	got := roundTrip(t, &statsv1.Snapshot{Connections: &statsv1.ConnectionsSection{Data: &statsv1.ConnectionsData{
		ListeningSockets: []*statsv1.ListeningSocket{
			{Local: &statsv1.Endpoint{Ip: "::", Port: proto.Uint32(8080)}, ProtocolNumber: 6},
			{Local: &statsv1.Endpoint{Ip: "2001:db8::1", Port: proto.Uint32(443)}, ProtocolNumber: 6, Owners: []*statsv1.ProcessOwner{
				{Command: proto.String("worker")}, {Pid: proto.Uint32(42), User: proto.String("service")},
			}},
		},
		TcpStates: []*statsv1.TcpStateCount{{State: "FUTURE_STATE", Count: 2}},
	}}})
	sockets := got.Connections.Data.ListeningSockets
	if len(sockets[0].Owners) != 0 || len(sockets[1].Owners) != 2 {
		t.Fatal("socket ownership changed")
	}
	if sockets[1].Owners[0].Pid != nil || sockets[1].Owners[1].Pid == nil || *sockets[1].Owners[1].Pid != 42 {
		t.Fatal("PID presence changed")
	}
	if got.Connections.Data.TcpStates[0].State != "FUTURE_STATE" {
		t.Fatal("unknown TCP state lost")
	}
}

func TestSnapshotMetadataRoundTrip(t *testing.T) {
	end := time.Date(2026, time.November, 5, 12, 0, 0, 0, time.UTC)
	got := roundTrip(t, &statsv1.Snapshot{
		WindowStart: timestamppb.New(end.Add(-15 * time.Second)), WindowEnd: timestamppb.New(end), GeneratedAt: timestamppb.New(end.Add(time.Second)),
		Traffic: &statsv1.TrafficSection{Status: &statsv1.CollectionStatus{State: statsv1.CollectionState_COLLECTION_STATE_PARTIAL, CoverageRatio: 0.5, Detail: "capture interrupted", LastSampleAt: timestamppb.New(end.Add(-time.Second))}, Data: &statsv1.TrafficData{}},
	})
	if got.Traffic.Status.LastSampleAt == nil || got.WindowStart == nil || got.WindowEnd == nil || got.GeneratedAt == nil {
		t.Fatal("metadata lost")
	}
}
