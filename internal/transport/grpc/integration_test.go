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
