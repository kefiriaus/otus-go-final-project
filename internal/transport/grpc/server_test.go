package grpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/daemon"
	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type providerFunc func(context.Context, aggregate.Window) (daemon.Snapshot, error)

func (f providerFunc) Snapshot(c context.Context, w aggregate.Window) (daemon.Snapshot, error) {
	return f(c, w)
}

type testStream struct {
	grpc.ServerStreamingServer[statsv1.Snapshot]
	ctx  context.Context
	send func(*statsv1.Snapshot) error
}

func (s testStream) Context() context.Context       { return s.ctx }
func (s testStream) Send(v *statsv1.Snapshot) error { return s.send(v) }

func subscribeAsync(
	s *Server,
	request *statsv1.SubscribeRequest,
	stream testStream,
) <-chan error {
	done := make(chan error, 1)
	go func() { done <- s.Subscribe(request, stream) }()
	return done
}

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeTimer
	created chan struct{}
}
type fakeTimer struct {
	ch       chan time.Time
	deadline time.Time
	stopped  bool
	clock    *fakeClock
}

func (f *fakeClock) Now() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.now }
func (f *fakeClock) NewTimer(d time.Duration) timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{ch: make(chan time.Time, 1), deadline: f.now.Add(d), clock: f}
	f.timers = append(f.timers, t)
	select {
	case f.created <- struct{}{}:
	default:
	}
	return t
}
func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() { t.clock.mu.Lock(); defer t.clock.mu.Unlock(); t.stopped = true }

func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	for _, t := range f.timers {
		if !t.stopped && !t.deadline.After(f.now) {
			t.stopped = true
			t.ch <- f.now
		}
	}
}

func newClock() *fakeClock {
	return &fakeClock{now: time.Unix(100, 0).UTC(), created: make(chan struct{}, 100)}
}

func waitValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}

func TestSubscriptionValidation(t *testing.T) {
	var calls atomic.Int32
	p := providerFunc(
		func(context.Context, aggregate.Window) (daemon.Snapshot, error) { calls.Add(1); return fixture(), nil },
	)
	s, err := NewServer(p, Options{MaxWindowSeconds: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, r := range []*statsv1.SubscribeRequest{
		nil,
		{},
		{IntervalSeconds: 3601, WindowSeconds: 1},
		{IntervalSeconds: 1, WindowSeconds: 11},
		{IntervalSeconds: 1, WindowSeconds: ^uint32(0)},
	} {
		err := s.Subscribe(r, testStream{ctx: context.Background()})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid request reached provider")
	}
}

func TestSubscriptionAnchoredWarmupAndLateTick(t *testing.T) {
	clock := newClock()
	windows := make(chan aggregate.Window, 10)
	s, _ := NewServer(
		providerFunc(func(_ context.Context, w aggregate.Window) (daemon.Snapshot, error) {
			windows <- w
			v := fixture()
			v.Window = w
			v.GeneratedAt = clock.Now()
			return v, nil
		}),
		Options{},
	)
	s.clock = clock
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sent := make(chan *statsv1.Snapshot, 10)
	done := make(chan error, 1)
	go func() {
		request := &statsv1.SubscribeRequest{IntervalSeconds: 2, WindowSeconds: 3}
		stream := testStream{
			ctx: ctx,
			send: func(v *statsv1.Snapshot) error {
				sent <- v
				return nil
			},
		}
		done <- s.Subscribe(request, stream)
	}()
	waitValue(t, clock.created)
	clock.advance(2 * time.Second)
	select {
	case <-windows:
		t.Fatal("early response")
	default:
	}
	clock.advance(time.Second)
	first := waitValue(t, sent)
	if first.WindowEnd.AsTime().Unix() != 103 {
		t.Fatal(first)
	}
	waitValue(t, clock.created)
	clock.advance(5 * time.Second)
	second := waitValue(t, sent)
	if second.WindowEnd.AsTime().Unix() != 107 {
		t.Fatal(second)
	}
	cancel()
	if status.Code(waitValue(t, done)) != codes.Canceled {
		t.Fatal("cancellation not preserved")
	}
}

func TestSubscriptionBeforeWarmupCancellationAndLimit(t *testing.T) {
	clock := newClock()
	s, _ := NewServer(
		providerFunc(func(context.Context, aggregate.Window) (daemon.Snapshot, error) {
			t.Error("before warmup")
			return fixture(), nil
		}),
		Options{MaxSubscriptions: 1},
	)
	s.clock = clock
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Subscribe(&statsv1.SubscribeRequest{IntervalSeconds: 10, WindowSeconds: 1}, testStream{ctx: ctx})
	}()
	waitValue(t, clock.created)
	err := s.Subscribe(
		&statsv1.SubscribeRequest{IntervalSeconds: 1, WindowSeconds: 1},
		testStream{ctx: ctx},
	)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	cancel()
	waitValue(t, done)
	if len(s.slots) != 0 {
		t.Fatal("permit leaked")
	}
}

func TestSubscriptionProviderFailure(t *testing.T) {
	clock := newClock()
	s, _ := NewServer(
		providerFunc(func(context.Context, aggregate.Window) (daemon.Snapshot, error) {
			return daemon.Snapshot{}, errors.New("broken")
		}),
		Options{},
	)
	s.clock = clock
	defer s.Close()
	done := subscribeAsync(
		s,
		&statsv1.SubscribeRequest{IntervalSeconds: 1, WindowSeconds: 1},
		testStream{ctx: context.Background()},
	)
	waitValue(t, clock.created)
	clock.advance(time.Second)
	if status.Code(waitValue(t, done)) != codes.Internal {
		t.Fatal("wrong provider failure")
	}
}

func waitDeadline(t *testing.T, c *fakeClock, unix int64) {
	t.Helper()
	limit := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		found := false
		for _, tm := range c.timers {
			if !tm.stopped && tm.deadline.Unix() == unix {
				found = true
			}
		}
		c.mu.Unlock()
		if found {
			return
		}
		if time.Now().After(limit) {
			t.Fatalf("no timer at %d", unix)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPendingSnapshotReplacesOlderAndClientIsIndependent(t *testing.T) {
	clock := newClock()
	built := make(chan aggregate.Window, 20)
	s, _ := NewServer(
		providerFunc(func(_ context.Context, w aggregate.Window) (daemon.Snapshot, error) {
			built <- w
			v := fixture()
			v.Window = w
			return v, nil
		}),
		Options{SendTimeout: 20 * time.Second},
	)
	s.clock = clock
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sent := make(chan int64, 10)
	release := make(chan struct{})
	done := subscribeAsync(s, &statsv1.SubscribeRequest{IntervalSeconds: 1, WindowSeconds: 1}, testStream{
		ctx: ctx,
		send: func(v *statsv1.Snapshot) error {
			sent <- v.WindowEnd.AsTime().Unix()
			if v.WindowEnd.AsTime().Unix() == 101 {
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
	})
	waitDeadline(t, clock, 101)
	clock.advance(time.Second)
	if waitValue(t, sent) != 101 {
		t.Fatal("first snapshot")
	}
	waitValue(t, built)
	waitDeadline(t, clock, 102)
	clock.advance(time.Second)
	waitValue(t, built)
	waitDeadline(t, clock, 103)
	clock.advance(time.Second)
	waitValue(t, built)
	waitDeadline(t, clock, 104)
	close(release)
	if got := waitValue(t, sent); got != 103 {
		t.Fatalf("sent stale snapshot %d", got)
	}
	cancel()
	waitValue(t, done)
}

func TestSendTimeoutReleasesSlot(t *testing.T) {
	clock := newClock()
	s, _ := NewServer(
		providerFunc(func(_ context.Context, w aggregate.Window) (daemon.Snapshot, error) {
			v := fixture()
			v.Window = w
			return v, nil
		}),
		Options{SendTimeout: 5 * time.Second, MaxSubscriptions: 1},
	)
	s.clock = clock
	defer s.Close()
	transportCtx, transportCancel := context.WithCancel(context.Background())
	defer transportCancel()
	started := make(chan struct{})
	finished := make(chan struct{})
	done := subscribeAsync(s, &statsv1.SubscribeRequest{IntervalSeconds: 10, WindowSeconds: 1}, testStream{
		ctx: transportCtx,
		send: func(*statsv1.Snapshot) error {
			close(started)
			<-transportCtx.Done()
			close(finished)
			return transportCtx.Err()
		},
	})
	waitDeadline(t, clock, 101)
	clock.advance(time.Second)
	waitValue(t, started)
	waitDeadline(t, clock, 106)
	clock.advance(5 * time.Second)
	if status.Code(waitValue(t, done)) != codes.DeadlineExceeded {
		t.Fatal("send deadline missing")
	}
	if len(s.slots) != 0 {
		t.Fatal("timeout retained permit")
	}
	transportCancel()
	waitValue(t, finished)
}

func TestBlockedClientDoesNotDelayHealthySubscriber(t *testing.T) {
	clock := newClock()
	s, _ := NewServer(
		providerFunc(func(_ context.Context, w aggregate.Window) (daemon.Snapshot, error) {
			v := fixture()
			v.Window = w
			return v, nil
		}),
		Options{SendTimeout: 20 * time.Second},
	)
	s.clock = clock
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slowStarted := make(chan struct{})
	slowDone := subscribeAsync(s, &statsv1.SubscribeRequest{IntervalSeconds: 1, WindowSeconds: 1}, testStream{
		ctx: ctx,
		send: func(*statsv1.Snapshot) error {
			close(slowStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	})
	waitDeadline(t, clock, 101)
	clock.advance(time.Second)
	waitValue(t, slowStarted)
	healthy := make(chan *statsv1.Snapshot, 10)
	healthyDone := subscribeAsync(s, &statsv1.SubscribeRequest{IntervalSeconds: 1, WindowSeconds: 1}, testStream{
		ctx: ctx,
		send: func(v *statsv1.Snapshot) error {
			healthy <- v
			return nil
		},
	})
	limit := time.Now().Add(2 * time.Second)
	for {
		clock.mu.Lock()
		count := 0
		for _, tm := range clock.timers {
			if !tm.stopped && tm.deadline.Unix() == 102 {
				count++
			}
		}
		clock.mu.Unlock()
		if count == 2 {
			break
		}
		if time.Now().After(limit) {
			t.Fatal("both subscribers did not schedule")
		}
		time.Sleep(time.Millisecond)
	}
	clock.advance(time.Second)
	got := waitValue(t, healthy)
	if got.WindowEnd.AsTime().Unix() != 102 {
		t.Fatal(got)
	}
	cancel()
	waitValue(t, slowDone)
	waitValue(t, healthyDone)
}
