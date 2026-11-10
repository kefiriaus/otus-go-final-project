package collector

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunGroupConcurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 2)
	finished := make(chan struct{}, 2)
	done := make(chan error, 1)
	worker := func(c context.Context) error {
		started <- struct{}{}
		<-c.Done()
		finished <- struct{}{}
		return c.Err()
	}
	go func() { done <- RunGroup(ctx, worker, worker) }()
	receive(t, started)
	receive(t, started)
	cancel()
	if !errors.Is(receive(t, done), context.Canceled) {
		t.Fatal("cancellation")
	}
	if len(finished) != 2 {
		t.Fatal("did not await workers")
	}
}
func TestRunGroupWorkerFailure(t *testing.T) {
	sentinel := errors.New("terminal")
	ready := make(chan struct{})
	cleaned := false
	err := RunGroup(context.Background(), func(c context.Context) error { close(ready); <-c.Done(); cleaned = true; return c.Err() }, func(context.Context) error { <-ready; return sentinel })
	if !errors.Is(err, sentinel) || !cleaned {
		t.Fatalf("result: %v, cleanup %v", err, cleaned)
	}
	if RunGroup(context.Background(), func(context.Context) error { return nil }) == nil {
		t.Fatal("unexpected return accepted")
	}
}
func TestRunGroupEmpty(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunGroup(ctx) }()
	select {
	case <-done:
		t.Fatal("returned early")
	case <-time.After(time.Millisecond):
	}
	cancel()
	if !errors.Is(receive(t, done), context.Canceled) {
		t.Fatal("cancellation")
	}
}
func TestRunGroupNilWorker(t *testing.T) {
	if RunGroup(context.Background(), func(context.Context) error { t.Error("started"); return nil }, nil) == nil {
		t.Fatal("accepted nil")
	}
}
func TestRunGroupPeriodicIsolation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	good := make(chan Sample[int])
	bad := make(chan Sample[int])
	done := make(chan error, 1)
	options := Options{5 * time.Millisecond, time.Millisecond}
	go func() {
		done <- RunGroup(ctx, func(c context.Context) error {
			return RunPeriodic(c, sourceFunc(func(c context.Context) (int, error) { <-c.Done(); return 0, c.Err() }), options, bad)
		}, func(c context.Context) error {
			return RunPeriodic(c, sourceFunc(func(context.Context) (int, error) { return 7, nil }), options, good)
		})
	}()
	if s := receive(t, bad); !errors.Is(s.Err, context.DeadlineExceeded) {
		t.Fatalf("bad: %+v", s)
	}
	for i := 0; i < 3; i++ {
		if s := receive(t, good); s.Value != 7 || s.Err != nil {
			t.Fatalf("good: %+v", s)
		}
	}
	cancel()
	if !errors.Is(receive(t, done), context.Canceled) {
		t.Fatal("cancellation")
	}
}
