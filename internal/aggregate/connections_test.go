package aggregate

import (
	"net/netip"
	"testing"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func connectionTestData(port uint16, count uint64) metrics.Connections {
	pid := uint32(42)
	command, user := "process", "2000"
	return metrics.Connections{
		ListeningSockets: []metrics.ListeningSocket{
			{
				Local: metrics.Endpoint{
					Address: netip.MustParseAddr("::"),
					Port:    port,
					HasPort: true,
				},
				Protocol: 6,
				Owners:   []metrics.ProcessOwner{{PID: &pid, Command: &command, User: &user}, {}},
			},
		},
		TCPStates: []metrics.TCPStateCount{{State: "UNKNOWN_FF", Count: count}},
	}
}

func TestLatestConnections(t *testing.T) {
	values := []Interval[metrics.Connections]{
		interval(0, 1, connectionTestData(80, 10)),
		interval(1, 2, connectionTestData(81, 20)),
	}
	got, err := LatestConnections(Window{at(0), at(2)}, values)
	if err != nil || got.Data == nil || got.Partial || got.CoverageRatio != 1 ||
		!got.LastSampleAt.Equal(at(1)) ||
		got.Data.TCPStates[0].Count != 20 ||
		got.Data.TCPStates[0].State != "UNKNOWN_FF" ||
		got.Data.ListeningSockets[0].Local.Port != 81 {
		t.Fatalf("%+v %v", got, err)
	}
	first, err := LatestConnections(Window{at(0), at(1)}, values)
	if err != nil || first.Data.TCPStates[0].Count != 10 {
		t.Fatalf("end boundary: %+v %v", first, err)
	}
	*got.Data.ListeningSockets[0].Owners[0].PID = 999
	*got.Data.ListeningSockets[0].Owners[0].Command = "changed"
	*got.Data.ListeningSockets[0].Owners[0].User = "changed"
	got.Data.TCPStates[0].Count = 999
	if *values[1].Value.ListeningSockets[0].Owners[0].PID != 42 ||
		*values[1].Value.ListeningSockets[0].Owners[0].Command != "process" ||
		*values[1].Value.ListeningSockets[0].Owners[0].User != "2000" ||
		values[1].Value.TCPStates[0].Count != 20 {
		t.Fatal("result aliases history")
	}
	values[1].Partial = true
	partial, err := LatestConnections(Window{at(0), at(3)}, values)
	if err != nil || !partial.Partial {
		t.Fatalf("%+v %v", partial, err)
	}
	closeTo(t, partial.CoverageRatio, 2.0/3)
	empty, err := LatestConnections(
		Window{at(0), at(1)},
		[]Interval[metrics.Connections]{interval(0, 1, metrics.Connections{})},
	)
	if err != nil || empty.Data == nil || empty.CoverageRatio != 1 || empty.Partial {
		t.Fatalf("empty success: %+v %v", empty, err)
	}
	unavailable, err := LatestConnections(Window{at(2), at(3)}, values)
	if err != nil || unavailable.Data != nil || unavailable.LastSampleAt != nil ||
		unavailable.CoverageRatio != 0 {
		t.Fatalf("%+v %v", unavailable, err)
	}
}

func TestLatestConnectionsInvalid(t *testing.T) {
	if _, err := LatestConnections(Window{}, nil); err == nil {
		t.Fatal("invalid window")
	}
	for _, mode := range []string{
		"address", "zone", "port", "protocol", "PID",
		"command", "user", "state", "duplicate state",
	} {
		value := connectionTestData(80, 10)
		switch mode {
		case "address":
			value.ListeningSockets[0].Local.Address = netip.Addr{}
		case "zone":
			value.ListeningSockets[0].Local.Address = netip.MustParseAddr("fe80::1%eth0")
		case "port":
			value.ListeningSockets[0].Local.HasPort = false
		case "protocol":
			value.ListeningSockets[0].Protocol = 0
		case "PID":
			*value.ListeningSockets[0].Owners[0].PID = 0
		case "command":
			*value.ListeningSockets[0].Owners[0].Command = string([]byte{255})
		case "user":
			*value.ListeningSockets[0].Owners[0].User = string([]byte{255})
		case "state":
			value.TCPStates[0].State = ""
		case "duplicate state":
			value.TCPStates = append(value.TCPStates, value.TCPStates[0])
		}
		if _, err := LatestConnections(
			Window{at(0), at(1)},
			[]Interval[metrics.Connections]{interval(0, 1, value)},
		); err == nil {
			t.Fatal(mode)
		}
	}
	value := connectionTestData(80, 1)
	value.ListeningSockets = append(value.ListeningSockets, value.ListeningSockets[0])
	if _, err := LatestConnections(
		Window{at(0), at(1)},
		[]Interval[metrics.Connections]{interval(0, 1, value)},
	); err != nil {
		t.Fatalf("distinct sockets may share endpoint: %v", err)
	}
}
