package grpc

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/daemon"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func startBufconn(t *testing.T, service statsv1.StatsServiceServer) statsv1.StatsServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	statsv1.RegisterStatsServiceServer(g, service)
	done := make(chan struct{})
	go func() { defer close(done); _ = g.Serve(listener) }()
	conn, err := grpc.NewClient(
		"passthrough:///test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(
			func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) },
		),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(8<<20)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); g.Stop(); listener.Close(); waitValue(t, done) })
	return statsv1.NewStatsServiceClient(conn)
}

type sendObserved struct {
	grpc.ServerStreamingServer[statsv1.Snapshot]
	started, finished chan struct{}
	count             int
}

func (s *sendObserved) Send(v *statsv1.Snapshot) error {
	s.count++
	if s.count == 2 {
		close(s.started)
		defer close(s.finished)
	}
	return s.ServerStreamingServer.Send(v)
}

type observedService struct {
	*Server
	started, finished, returned chan struct{}
}

func (s observedService) Subscribe(
	r *statsv1.SubscribeRequest,
	stream grpc.ServerStreamingServer[statsv1.Snapshot],
) error {
	defer close(s.returned)
	return s.Server.Subscribe(
		r,
		&sendObserved{ServerStreamingServer: stream, started: s.started, finished: s.finished},
	)
}

func TestRealGRPCSendUnwindsAfterTimeout(t *testing.T) {
	clock := newClock()
	at := clock.Now()
	command := strings.Repeat("x", 1024)
	data := metrics.Connections{}
	for i := 0; i < 512; i++ {
		socket := metrics.ListeningSocket{
			Local: metrics.Endpoint{
				Address: netip.MustParseAddr("127.0.0.1"),
				HasPort: true,
				Port:    uint16(i),
			},
			Protocol: 6,
		}
		for j := 0; j < 8; j++ {
			socket.Owners = append(socket.Owners, metrics.ProcessOwner{Command: &command})
		}
		data.ListeningSockets = append(data.ListeningSockets, socket)
	}
	s, _ := NewServer(
		providerFunc(func(_ context.Context, w aggregate.Window) (daemon.Snapshot, error) {
			v := fixture()
			v.Window = w
			v.Connections = daemon.Section[metrics.Connections]{
				Enabled: true,
				Summary: aggregate.Summary[metrics.Connections]{
					Data:          &data,
					CoverageRatio: 1,
					LastSampleAt:  &at,
				},
			}
			return v, nil
		}),
		Options{SendTimeout: 5 * time.Second},
	)
	s.clock = clock
	defer s.Close()
	observed := observedService{s, make(chan struct{}), make(chan struct{}), make(chan struct{})}
	client := startBufconn(t, observed)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := client.Subscribe(
		ctx,
		&statsv1.SubscribeRequest{IntervalSeconds: 1, WindowSeconds: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	waitDeadline(t, clock, 101)
	clock.advance(time.Second)
	waitDeadline(t, clock, 102)
	clock.advance(time.Second)
	waitValue(t, observed.started)
	waitDeadline(t, clock, 107)
	select {
	case <-observed.finished:
		t.Fatal("large send unexpectedly completed without receiver")
	case <-time.After(10 * time.Millisecond):
	}
	clock.advance(5 * time.Second)
	waitValue(t, observed.returned)
	waitValue(t, observed.finished)
	for i := 0; i < 3; i++ {
		_, err = stream.Recv()
		if err != nil {
			break
		}
	}
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("RPC returned %v", err)
	}
}

func TestRealGRPCIndependentSubscriptions(t *testing.T) {
	clock := newClock()
	s, _ := NewServer(
		providerFunc(func(_ context.Context, w aggregate.Window) (daemon.Snapshot, error) {
			v := fixture()
			v.Window = w
			at := w.Start
			v.CPU.Enabled = true
			v.CPU.Detail = "source unavailable"
			v.LoadAverage = daemon.Section[metrics.LoadAverage]{
				Enabled: true,
				Summary: aggregate.Summary[metrics.LoadAverage]{
					Data:          &metrics.LoadAverage{Load1: 2},
					CoverageRatio: 1,
					LastSampleAt:  &at,
				},
			}
			return v, nil
		}),
		Options{},
	)
	s.clock = clock
	defer s.Close()
	client := startBufconn(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	aCtx, aCancel := context.WithCancel(ctx)
	defer aCancel()
	a, err := client.Subscribe(
		aCtx,
		&statsv1.SubscribeRequest{IntervalSeconds: 2, WindowSeconds: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	b, err := client.Subscribe(ctx, &statsv1.SubscribeRequest{IntervalSeconds: 1, WindowSeconds: 2})
	if err != nil {
		t.Fatal(err)
	}
	waitDeadline(t, clock, 101)
	waitDeadline(t, clock, 102)
	clock.advance(time.Second)
	first, err := a.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.WindowStart.AsTime().Unix() != 100 || first.WindowEnd.AsTime().Unix() != 101 ||
		first.LoadAverage.Data.Load1 != 2 ||
		first.Cpu.Status.State != statsv1.CollectionState_COLLECTION_STATE_UNAVAILABLE ||
		first.DiskIo.Status.State != statsv1.CollectionState_COLLECTION_STATE_DISABLED ||
		first.Filesystems == nil ||
		first.Traffic == nil ||
		first.Connections == nil {
		t.Fatal(first)
	}
	clock.advance(time.Second)
	second, err := b.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if second.WindowEnd.AsTime().Unix() != 102 || second.WindowStart.AsTime().Unix() != 100 {
		t.Fatal(second)
	}
	aCancel()
	_, err = a.Recv()
	if status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	waitDeadline(t, clock, 103)
	clock.advance(time.Second)
	third, err := b.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if third.WindowEnd.AsTime().Unix() != 103 || third.WindowStart.AsTime().Unix() != 101 {
		t.Fatal(third)
	}
	s.Close()
	_, err = b.Recv()
	if status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
}

type loadClient struct {
	stream grpc.ServerStreamingClient[statsv1.Snapshot]
	cancel context.CancelFunc
}

type loadResult struct {
	index int
	end   int64
	err   error
}

func newLoadClients(t *testing.T, client statsv1.StatsServiceClient, count int) []loadClient {
	t.Helper()
	clients := make([]loadClient, 0, count)
	for i := 0; i < count; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		stream, err := client.Subscribe(ctx, &statsv1.SubscribeRequest{IntervalSeconds: 1, WindowSeconds: 1})
		if err != nil {
			cancel()
			for _, existing := range clients {
				existing.cancel()
			}
			t.Fatal(err)
		}
		clients = append(clients, loadClient{stream: stream, cancel: cancel})
	}
	return clients
}

func receiveLoadSnapshots(clients []loadClient) <-chan loadResult {
	results := make(chan loadResult, len(clients))
	for i, client := range clients {
		go func() {
			snapshot, err := client.stream.Recv()
			result := loadResult{index: i, err: err}
			if err == nil {
				result.end = snapshot.WindowEnd.AsTime().Unix()
			}
			results <- result
		}()
	}
	return results
}

func checkLoadSnapshots(t *testing.T, results <-chan loadResult, count int, wantEnd int64) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf("client %d: %v", result.index, result.err)
			}
			if result.end != wantEnd {
				t.Fatalf("client %d: window end %d, want %d", result.index, result.end, wantEnd)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("received %d of %d snapshots", i, count)
		}
	}
}

func expectLoadCanceled(t *testing.T, clients []loadClient) {
	t.Helper()
	for i, client := range clients {
		done := make(chan error, 1)
		go func() {
			_, err := client.stream.Recv()
			done <- err
		}()
		select {
		case err := <-done:
			if status.Code(err) != codes.Canceled {
				t.Fatalf("client %d: expected cancellation, got %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("client %d: cancellation did not arrive", i)
		}
	}
}

func cancelLoadClients(t *testing.T, clients []loadClient) {
	t.Helper()
	for _, client := range clients {
		client.cancel()
	}
	expectLoadCanceled(t, clients)
}

func waitForSlotCount(t *testing.T, server *Server, want int) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if len(server.slots) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot count %d, want %d", len(server.slots), want)
		}
		<-ticker.C
	}
}

func waitForLoadTimers(t *testing.T, clock *fakeClock, want int, deadline int64) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	limit := time.Now().Add(5 * time.Second)
	for {
		clock.mu.Lock()
		count := 0
		for _, timer := range clock.timers {
			if !timer.stopped && timer.deadline.Unix() == deadline {
				count++
			}
		}
		clock.mu.Unlock()
		if count == want {
			return
		}
		if time.Now().After(limit) {
			t.Fatalf("found %d timers at %d, want %d", count, deadline, want)
		}
		<-ticker.C
	}
}

func TestRealGRPCConcurrentSubscriberLoad(t *testing.T) {
	clock := newClock()
	s, _ := NewServer(providerFunc(func(_ context.Context, w aggregate.Window) (daemon.Snapshot, error) {
		value := fixture()
		value.Window = w
		value.GeneratedAt = clock.Now()
		return value, nil
	}), Options{})
	s.clock = clock
	defer s.Close()
	client := startBufconn(t, s)
	clients := newLoadClients(t, client, 32)
	waitForSlotCount(t, s, 32)
	defer func() {
		for _, client := range clients {
			client.cancel()
		}
	}()
	extraCtx, extraCancel := context.WithCancel(context.Background())
	defer extraCancel()
	extra, err := client.Subscribe(extraCtx, &statsv1.SubscribeRequest{IntervalSeconds: 1, WindowSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := extra.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("33rd subscription returned %v", err)
	}
	waitForLoadTimers(t, clock, 32, 101)
	results := receiveLoadSnapshots(clients)
	clock.advance(time.Second)
	checkLoadSnapshots(t, results, len(clients), 101)
	cancelLoadClients(t, clients[:16])
	waitForSlotCount(t, s, 16)
	active := clients[16:]
	active = append(active, newLoadClients(t, client, 16)...)
	defer func() {
		for _, client := range active[16:] {
			client.cancel()
		}
	}()
	waitForLoadTimers(t, clock, 32, 102)
	results = receiveLoadSnapshots(active)
	clock.advance(time.Second)
	checkLoadSnapshots(t, results, len(active), 102)
	s.Close()
	expectLoadCanceled(t, active)
	waitForSlotCount(t, s, 0)
}
