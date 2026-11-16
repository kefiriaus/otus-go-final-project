package main

import (
	"strings"
	"testing"

	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/protobuf/proto"
)

const (
	testIPv4Address = "192.0.2.1"
	testIPv6Address = "2001:db8::1"
	testLocalhost   = "127.0.0.1"
)

func TestFormatEndpoint(t *testing.T) {
	for _, tt := range []struct {
		name     string
		endpoint *statsv1.Endpoint
		want     string
	}{
		{"IPv4", &statsv1.Endpoint{Ip: testIPv4Address, Port: proto.Uint32(80)}, "192.0.2.1:80"},
		{"IPv6", &statsv1.Endpoint{Ip: testIPv6Address, Port: proto.Uint32(80)}, "[2001:db8::1]:80"},
		{"IPv4 no port", &statsv1.Endpoint{Ip: testIPv4Address}, testIPv4Address},
		{"IPv6 no port", &statsv1.Endpoint{Ip: testIPv6Address}, testIPv6Address},
		{"zero port", &statsv1.Endpoint{Ip: testIPv4Address, Port: proto.Uint32(0)}, "192.0.2.1:0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := formatEndpoint(tt.endpoint)
			if err != nil || got != tt.want {
				t.Fatalf("got %q, %v; want %q", got, err, tt.want)
			}
		})
	}
	for _, endpoint := range []*statsv1.Endpoint{
		nil, {}, {Ip: "invalid"}, {Ip: "fe80::1%eth0"},
	} {
		if _, err := formatEndpoint(endpoint); err == nil {
			t.Fatalf("accepted %+v", endpoint)
		}
	}
}

func TestFormatTraffic(t *testing.T) {
	section := &statsv1.TrafficSection{
		Status: okStatus(),
		Data: &statsv1.TrafficData{
			Protocols: []*statsv1.ProtocolTraffic{{ProtocolNumber: 6, Bytes: 100, Percent: 100}},
			Flows: []*statsv1.FlowTraffic{
				{
					Source:         &statsv1.Endpoint{Ip: testIPv4Address, Port: proto.Uint32(123)},
					Destination:    &statsv1.Endpoint{Ip: "198.51.100.2", Port: proto.Uint32(53)},
					ProtocolNumber: 17, BytesPerSecond: 10,
				},
				{
					Source:         &statsv1.Endpoint{Ip: testIPv6Address},
					Destination:    &statsv1.Endpoint{Ip: "2001:db8::2"},
					ProtocolNumber: 58, BytesPerSecond: 20,
				},
			},
		},
	}
	got, err := formatTraffic(section)
	if err != nil {
		t.Fatal(err)
	}
	want := "traffic: OK coverage=100.00%\n" +
		"  protocol=6 bytes=100 percent=100.00%\n" +
		"  flow=192.0.2.1:123 -> 198.51.100.2:53 protocol=17 bytes_s=10.00\n" +
		"  flow=2001:db8::1 -> 2001:db8::2 protocol=58 bytes_s=20.00\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	section.Data = &statsv1.TrafficData{}
	got, err = formatTraffic(section)
	if err != nil {
		t.Fatal(err)
	}
	want = "traffic: OK coverage=100.00%\n  protocols: empty\n  flows: empty\n"
	if got != want {
		t.Fatalf("empty output %q", got)
	}
}

func TestFormatConnections(t *testing.T) {
	section := &statsv1.ConnectionsSection{
		Status: okStatus(),
		Data: &statsv1.ConnectionsData{
			ListeningSockets: []*statsv1.ListeningSocket{
				{
					Local: &statsv1.Endpoint{Ip: testLocalhost, Port: proto.Uint32(80)}, ProtocolNumber: 6,
					Owners: []*statsv1.ProcessOwner{
						{Pid: proto.Uint32(42), Command: proto.String("server"), User: proto.String("1000")},
						{Pid: proto.Uint32(43), Command: proto.String("worker process"), User: proto.String("2000")},
					},
				},
				{
					Local: &statsv1.Endpoint{Ip: testLocalhost, Port: proto.Uint32(81)}, ProtocolNumber: 17,
					Owners: []*statsv1.ProcessOwner{{}},
				},
				{
					Local: &statsv1.Endpoint{Ip: testLocalhost, Port: proto.Uint32(82)}, ProtocolNumber: 6,
				},
			},
			TcpStates: []*statsv1.TcpStateCount{{State: "UNKNOWN_FF", Count: 2}},
		},
	}
	got, err := formatConnections(section)
	if err != nil {
		t.Fatal(err)
	}
	want := "connections: OK coverage=100.00%\n" +
		"  socket=127.0.0.1:80 protocol=6 owners=[pid=42 command=\"server\" user=\"1000\";" +
		" pid=43 command=\"worker process\" user=\"2000\"]\n" +
		"  socket=127.0.0.1:81 protocol=17 owners=[pid=n/a command=n/a user=n/a]\n" +
		"  socket=127.0.0.1:82 protocol=6 owners=[]\n" +
		"  tcp_state=UNKNOWN_FF count=2\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	section.Data = &statsv1.ConnectionsData{}
	got, err = formatConnections(section)
	if err != nil {
		t.Fatal(err)
	}
	want = "connections: OK coverage=100.00%\n  listening_sockets: empty\n  tcp_states: empty\n"
	if got != want {
		t.Fatalf("empty output %q", got)
	}
}

func TestFormatSnapshotUsesNetworkSections(t *testing.T) {
	snapshot := clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_DISABLED)
	snapshot.Traffic = &statsv1.TrafficSection{Status: okStatus(), Data: &statsv1.TrafficData{}}
	snapshot.Connections = &statsv1.ConnectionsSection{
		Status: okStatus(), Data: &statsv1.ConnectionsData{},
	}
	got, err := formatSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"  protocols: empty\n  flows: empty\n",
		"  listening_sockets: empty\n  tcp_states: empty\n",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("snapshot missing %q:\n%s", expected, got)
		}
	}
}
