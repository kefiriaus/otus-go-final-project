package collector

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"testing"
	"testing/fstest"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const (
	testIPv4Loopback = "127.0.0.1"
	testIPv6Loopback = "::1"
)

type connectionsWindowCase struct {
	start, end int
	port       uint16
	coverage   float64
	partial    bool
}

func assertConnectionsWindow(
	t *testing.T,
	series *aggregate.GaugeSeries[metrics.Connections],
	base time.Time,
	tc connectionsWindowCase,
) {
	t.Helper()
	window := aggregate.Window{
		Start: base.Add(time.Duration(tc.start) * time.Second),
		End:   base.Add(time.Duration(tc.end) * time.Second),
	}
	values, err := series.Read(window)
	if err != nil {
		t.Fatal(err)
	}
	got, err := aggregate.LatestConnections(window, values)
	if err != nil || got.Data == nil || got.Partial != tc.partial ||
		got.CoverageRatio != tc.coverage || len(got.Data.ListeningSockets) != 1 ||
		got.Data.ListeningSockets[0].Local.Port != tc.port {
		t.Fatalf("%+v %v", got, err)
	}
	if tc.end == 2 {
		if len(got.Data.ListeningSockets[0].Owners) != 0 || len(got.Data.TCPStates) != 2 ||
			got.Data.TCPStates[1].State != "UNKNOWN_FF" {
			t.Fatal(got.Data)
		}
	} else if len(got.Data.ListeningSockets[0].Owners) != 2 {
		t.Fatal("owner recovery failed")
	}
	again, _ := series.Read(window)
	repeated, err := aggregate.LatestConnections(window, again)
	if err != nil || repeated.Data == nil || repeated.Data.ListeningSockets[0].Local.Port != tc.port {
		t.Fatal("independent window read consumed data")
	}
}

func TestConnectionsHistoryIntegration(t *testing.T) {
	source, _ := NewConnectionsSource(DefaultConnectionsOptions())
	series, _ := aggregate.NewGaugeSeries[metrics.Connections](5)
	base := time.Unix(1000, 0).UTC()
	for i := 0; i < 4; i++ {
		fixture := connectionsFixture()
		port := uint16(80)
		if i == 3 {
			port = 81
		}
		fixture["self/net/tcp"].Data = []byte(
			socketTableHeader + socketLine(
				0,
				procSocketEndpoint(testIPv4Loopback, port),
				procSocketEndpoint("0.0.0.0", 0),
				"0A",
				123,
			),
		)
		source.procFS = fixture
		if i == 1 {
			delete(fixture, "self/net/tcp6")
			fixture["self/net/tcp"].Data = append(
				fixture["self/net/tcp"].Data,
				[]byte(
					socketLine(
						1,
						procSocketEndpoint(testIPv4Loopback, 1234),
						procSocketEndpoint(testIPv4Loopback, 1235),
						"FF",
						0,
					),
				)...)
			source.procFS = ownerTestFS{
				FS:   fixture,
				link: func(string) (string, error) { return "", fs.ErrPermission },
			}
		}
		if i == 2 {
			source.procFS = fstest.MapFS{}
		}
		observation, err := source.Collect(context.Background())
		if i == 2 {
			if !errors.Is(err, fs.ErrNotExist) {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if err := series.Add(
			aggregate.Event[metrics.Connections]{
				At:      base.Add(time.Duration(i) * time.Second),
				Value:   observation.Data,
				Partial: observation.Partial,
				Err:     err,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []connectionsWindowCase{
		{0, 1, 80, 1, false}, {0, 2, 80, 1, true}, {2, 4, 81, .5, true},
	} {
		assertConnectionsWindow(t, series, base, tc)
	}
}

func TestConnectionsRealLinux(t *testing.T) {
	source, _ := NewConnectionsSource(DefaultConnectionsOptions())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := source.Collect(ctx)
	if err != nil {
		allAbsent := true
		for _, table := range []string{"tcp", "tcp6", "udp", "udp6"} {
			if _, cause := os.Stat("/proc/self/net/" + table); !errors.Is(cause, fs.ErrNotExist) {
				allAbsent = false
			}
		}
		if allAbsent {
			t.Skipf("procfs networking tables absent: %v", err)
		}
		t.Fatal(err)
	}
	t.Logf(
		"real Linux socket readings: %d bound/listening sockets, %d TCP states, partial=%v, detail=%s",
		len(got.Data.ListeningSockets),
		len(got.Data.TCPStates),
		got.Partial,
		got.Detail,
	)
}

func testPort(port int) uint16 {
	if port < 0 || port > 65535 {
		panic("invalid test port")
	}
	// #nosec G115 -- The range is checked immediately above.
	return uint16(port)
}

type wantedSocket struct {
	protocol uint8
	port     uint16
	ip       string
}

func checkOwnedLocalSockets(
	t *testing.T,
	got metrics.ConnectionsObservation,
	targets []wantedSocket,
) int {
	t.Helper()
	// #nosec G115 -- Linux PIDs fit in uint32.
	ownPID := uint32(os.Getpid())
	foundOwners := 0
	for _, target := range targets {
		found := false
		for _, socket := range got.Data.ListeningSockets {
			if socket.Protocol != target.protocol || socket.Local.Port != target.port ||
				socket.Local.Address.String() != target.ip {
				continue
			}
			found = true
			owns := false
			for _, owner := range socket.Owners {
				if owner.PID != nil && *owner.PID == ownPID {
					owns = true
				}
			}
			if len(socket.Owners) > 0 && !owns {
				t.Fatalf("local socket attributed to another PID: %+v", socket)
			}
			if owns {
				foundOwners++
			} else {
				if !got.Partial {
					t.Fatal("unavailable owner not marked partial")
				}
				t.Logf("owner inaccessible for protocol%d port%d", target.protocol, target.port)
			}
		}
		if !found {
			t.Fatalf(
				"local protocol%d port%d absent; partial=%v detail=%s",
				target.protocol, target.port, got.Partial, got.Detail,
			)
		}
	}
	return foundOwners
}

func TestConnectionsLocalSocketSmoke(t *testing.T) {
	if os.Getenv("OTUS_CONNECTIONS_SMOKE") != "1" {
		t.Skip("set OTUS_CONNECTIONS_SMOKE=1 for local TCP/UDP sockets")
	}
	var listenConfig net.ListenConfig
	tcp, err := listenConfig.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("local TCP bind unavailable: %v", err)
	}
	defer tcp.Close()
	udp, err := listenConfig.ListenPacket(context.Background(), "udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	var dialer net.Dialer
	connected, err := dialer.DialContext(context.Background(), "udp4", udp.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer connected.Close()
	targets := []wantedSocket{
		{6, testPort(tcp.Addr().(*net.TCPAddr).Port), testIPv4Loopback},
		{17, testPort(udp.LocalAddr().(*net.UDPAddr).Port), testIPv4Loopback},
		{17, testPort(connected.LocalAddr().(*net.UDPAddr).Port), testIPv4Loopback},
	}
	tcp6, err := listenConfig.Listen(context.Background(), "tcp6", "[::1]:0")
	if err == nil {
		defer tcp6.Close()
		targets = append(
			targets,
			wantedSocket{6, testPort(tcp6.Addr().(*net.TCPAddr).Port), testIPv6Loopback},
		)
	} else {
		t.Logf("IPv6 local bind unavailable: %v", err)
	}
	source, _ := NewConnectionsSource(DefaultConnectionsOptions())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := source.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// #nosec G115 -- Linux PIDs fit in uint32.
	foundOwners := checkOwnedLocalSockets(t, got, targets)
	t.Logf(
		"verified %d local sockets, %d matched this PID; partial=%v detail=%s",
		len(targets),
		foundOwners,
		got.Partial,
		got.Detail,
	)
}
