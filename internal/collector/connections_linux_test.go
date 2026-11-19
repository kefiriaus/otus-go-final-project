package collector

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"
)

const (
	testCloseMode     = "close"
	testReadMode      = "read"
	testOpenMode      = "open"
	testParseMode     = "parse"
	testOversizedMode = "oversized"
)

func procSocketEndpoint(ip string, port uint16) string {
	address := netip.MustParseAddr(ip)
	raw := address.AsSlice()
	var out strings.Builder
	for i := 0; i < len(raw); i += 4 {
		fmt.Fprintf(&out, "%08X", binary.NativeEndian.Uint32(raw[i:i+4]))
	}
	fmt.Fprintf(&out, ":%04X", port)
	return out.String()
}

func connectionsFixture() fstest.MapFS {
	result := ownerFixture()
	for _, table := range []string{"tcp", "tcp6", "udp", "udp6"} {
		result["self/net/"+table] = &fstest.MapFile{Data: []byte(socketTableHeader)}
	}
	return result
}

func TestConnectionsSource(t *testing.T) {
	fixture := connectionsFixture()
	local := procSocketEndpoint("127.0.0.1", 8080)
	remote := procSocketEndpoint("0.0.0.0", 0)
	fixture["self/net/tcp"].Data = []byte(
		socketTableHeader + socketLine(
			0,
			local,
			remote,
			"0A",
			123,
		) + socketLine(
			1,
			local,
			remote,
			"0A",
			124,
		) + socketLine(
			2,
			local,
			remote,
			"01",
			125,
		) + socketLine(
			3,
			local,
			remote,
			"FF",
			0,
		),
	)
	fixture["self/net/udp"].Data = []byte(
		socketTableHeader + socketLine(
			0,
			procSocketEndpoint("0.0.0.0", 53),
			remote,
			"07",
			126,
		) + socketLine(
			1,
			procSocketEndpoint("127.0.0.1", 54),
			procSocketEndpoint("127.0.0.1", 55),
			"01",
			127,
		) + socketLine(
			2,
			remote,
			remote,
			"07",
			0,
		),
	)
	fixture["self/net/tcp6"].Data = []byte(
		socketTableHeader + socketLine(
			0,
			procSocketEndpoint("::1", 8081),
			procSocketEndpoint("::", 0),
			"0A",
			128,
		),
	)
	for i, inode := range []int{124, 126, 127, 128} {
		fixture[fmt.Sprintf("42/fd/%d", 6+i)] = &fstest.MapFile{
			Mode: fs.ModeSymlink,
			Data: []byte(fmt.Sprintf("socket:[%d]", inode)),
		}
	}
	source, _ := NewConnectionsSource(DefaultConnectionsOptions())
	source.procFS = fixture
	got, err := source.Collect(context.Background())
	if err != nil || got.Partial || len(got.Data.ListeningSockets) != 5 ||
		len(got.Data.TCPStates) != 3 {
		t.Fatalf("%+v %v", got, err)
	}
	counts := map[string]uint64{}
	for _, state := range got.Data.TCPStates {
		counts[state.State] = state.Count
	}
	if counts["LISTEN"] != 3 || counts["ESTABLISHED"] != 1 || counts["UNKNOWN_FF"] != 1 {
		t.Fatal(counts)
	}
	sameEndpoint := 0
	for _, socket := range got.Data.ListeningSockets {
		if !socket.Local.HasPort || len(socket.Owners) == 0 {
			t.Fatalf("%+v", socket)
		}
		if socket.Local.Port == 8080 {
			sameEndpoint++
		}
	}
	if sameEndpoint != 2 {
		t.Fatal("merged distinct sockets sharing one endpoint")
	}
}

func TestConnectionsSourcePartialAndEmpty(t *testing.T) {
	source, _ := NewConnectionsSource(DefaultConnectionsOptions())
	fixture := connectionsFixture()
	source.procFS = fixture
	empty, err := source.Collect(context.Background())
	if err != nil || empty.Partial || len(empty.Data.ListeningSockets) != 0 ||
		len(empty.Data.TCPStates) != 0 {
		t.Fatalf("%+v %v", empty, err)
	}
	delete(fixture, "self/net/tcp6")
	partial, err := source.Collect(context.Background())
	if err != nil || !partial.Partial || partial.Detail == "" {
		t.Fatalf("%+v %v", partial, err)
	}
	source.procFS = fstest.MapFS{}
	if _, err := source.Collect(context.Background()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	fixture = connectionsFixture()
	fixture["self/net/tcp"].Data = []byte("malformed")
	source.procFS = fixture
	partial, err = source.Collect(context.Background())
	if err != nil || !partial.Partial {
		t.Fatalf("%+v %v", partial, err)
	}
	fixture = connectionsFixture()
	fixture["self/net/tcp"].Data = []byte(
		socketTableHeader + socketLine(
			0,
			procSocketEndpoint("0.0.0.0", 80),
			procSocketEndpoint("0.0.0.0", 0),
			"0A",
			999,
		),
	)
	source.procFS = fixture
	partial, err = source.Collect(context.Background())
	if err != nil || !partial.Partial || len(partial.Data.ListeningSockets) != 1 ||
		len(partial.Data.ListeningSockets[0].Owners) != 0 {
		t.Fatalf("%+v %v", partial, err)
	}
}

func TestConnectionsSourceLimitsAndDuplicate(t *testing.T) {
	o := DefaultConnectionsOptions()
	o.MaxListeningSockets = 1
	source, _ := NewConnectionsSource(o)
	fixture := connectionsFixture()
	row := socketLine(
		0,
		procSocketEndpoint("0.0.0.0", 80),
		procSocketEndpoint("0.0.0.0", 0),
		"0A",
		123,
	)
	fixture["self/net/tcp"].Data = []byte(
		socketTableHeader + row + socketLine(
			1,
			procSocketEndpoint("0.0.0.0", 81),
			procSocketEndpoint("0.0.0.0", 0),
			"0A",
			124,
		),
	)
	source.procFS = fixture
	got, err := source.Collect(context.Background())
	if err != nil || !got.Partial || len(got.Data.ListeningSockets) != 1 ||
		got.Data.TCPStates[0].Count != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	fixture["self/net/tcp"].Data = []byte(socketTableHeader + row + row)
	got, err = source.Collect(context.Background())
	if err != nil || !got.Partial || got.Data.TCPStates[0].Count != 1 {
		t.Fatalf("duplicate inode counted twice: %+v %v", got, err)
	}
	o = DefaultConnectionsOptions()
	o.MaxSocketRows = 1
	source, _ = NewConnectionsSource(o)
	fixture = connectionsFixture()
	fixture["self/net/tcp"].Data = []byte(socketTableHeader + row)
	fixture["self/net/udp"].Data = []byte(socketTableHeader + row)
	source.procFS = fixture
	if _, err := source.Collect(context.Background()); err == nil {
		t.Fatal("combined row cap")
	}
	fixture["self/net/tcp"].Data = []byte(socketTableHeader + row + row)
	if _, err := source.Collect(context.Background()); err == nil {
		t.Fatal("per-table row cap")
	}
	for _, mutate := range []func(*ConnectionsOptions){
		func(o *ConnectionsOptions) { o.MaxSocketRows = 0 },
		func(o *ConnectionsOptions) { o.MaxProcesses = 8193 },
		func(o *ConnectionsOptions) { o.MaxFDs = 131073 },
		func(o *ConnectionsOptions) { o.MaxFDsPerProcess = 8193 },
		func(o *ConnectionsOptions) { o.MaxOwnersPerSocket = 33 },
		func(o *ConnectionsOptions) { o.OwnerTimeout = 0 },
	} {
		o := DefaultConnectionsOptions()
		mutate(&o)
		if _, err := NewConnectionsSource(o); err == nil {
			t.Fatal("invalid options")
		}
	}
}

func TestConnectionsSourceConcurrentAndCancellation(t *testing.T) {
	source, _ := NewConnectionsSource(DefaultConnectionsOptions())
	fixture := connectionsFixture()
	var opens atomic.Int32
	barrier := make(chan struct{})
	source.procFS = ownerTestFS{FS: fixture, open: func(name string) (fs.File, error) {
		if strings.HasPrefix(name, "self/net/") {
			if opens.Add(1) == 4 {
				close(barrier)
			}
			select {
			case <-barrier:
			case <-time.After(time.Second):
				return nil, errors.New("table readers not concurrent")
			}
		}
		return fixture.Open(name)
	}}
	if _, err := source.Collect(context.Background()); err != nil || opens.Load() != 4 {
		t.Fatalf("%d %v", opens.Load(), err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opens.Store(0)
	if _, err := source.Collect(ctx); !errors.Is(err, context.Canceled) || opens.Load() != 0 {
		t.Fatal(err)
	}
}

type connectionCloseFile struct {
	fs.File
	closed *atomic.Int32
	cancel context.CancelFunc
}

func (f *connectionCloseFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	if f.cancel != nil {
		f.cancel()
	}
	return n, err
}
func (f *connectionCloseFile) Close() error { f.closed.Add(1); return f.File.Close() }
func TestConnectionsSourceCancelsAndJoinsReaders(t *testing.T) {
	fixture := connectionsFixture()
	source, _ := NewConnectionsSource(DefaultConnectionsOptions())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var opened, closed atomic.Int32
	barrier := make(chan struct{})
	source.procFS = ownerTestFS{FS: fixture, open: func(name string) (fs.File, error) {
		file, err := fixture.Open(name)
		if err != nil {
			return nil, err
		}
		if opened.Add(1) == 4 {
			close(barrier)
		}
		<-barrier
		return &connectionCloseFile{File: file, closed: &closed, cancel: cancel}, nil
	}}
	_, err := source.Collect(ctx)
	if !errors.Is(err, context.Canceled) || opened.Load() != 4 || closed.Load() != 4 {
		t.Fatalf("%v opened=%d closed=%d", err, opened.Load(), closed.Load())
	}
}

func TestConnectionFileReadBoundsAndErrors(t *testing.T) {
	source, _ := NewConnectionsSource(DefaultConnectionsOptions())
	sentinel := errors.New("file failure")
	for _, mode := range []string{"bound", testReadMode, testCloseMode} {
		reader := strings.NewReader(strings.Repeat("x", 200))
		consumed := 0
		file := &diskFile{Reader: diskReader(func(p []byte) (int, error) {
			n, err := reader.Read(p)
			consumed += n
			if mode == testReadMode {
				err = sentinel
			}
			return n, err
		})}
		if mode == testCloseMode {
			file.closeErr = sentinel
		}
		source.procFS = diskFS(func(string) (fs.File, error) { return file, nil })
		_, err := source.readFile(context.Background(), "fixture", 128)
		if err == nil || consumed > 129 || !file.closed {
			t.Fatalf("%s: %v consumed=%d closed=%v", mode, err, consumed, file.closed)
		}
		if mode != "bound" && !errors.Is(err, sentinel) {
			t.Fatal("lost read/close cause")
		}
	}
}
