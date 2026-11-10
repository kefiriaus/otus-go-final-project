package collector

import (
	"context"
	"errors"
	"testing"
	"time"
)

type sourceFunc func(context.Context) (int, error)

func (f sourceFunc) Collect(ctx context.Context) (int, error) { return f(ctx) }
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
		var v T
		return v
	}
}
func TestRunPeriodicErrorThenRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan Sample[int])
	done := make(chan error, 1)
	sentinel := errors.New("source failure")
	calls := 0
	go func() {
		done <- RunPeriodic(ctx, sourceFunc(func(context.Context) (int, error) {
			calls++
			if calls == 1 {
				return 42, sentinel
			}
			return 7, nil
		}), Options{10 * time.Millisecond, time.Second}, out)
	}()
	first := receive(t, out)
	if !errors.Is(first.Err, sentinel) || first.Value != 0 {
		t.Fatalf("first: %+v", first)
	}
	second := receive(t, out)
	if second.Err != nil || second.Value != 7 || second.StartedAt.Location() != time.UTC || second.FinishedAt.Before(second.StartedAt) {
		t.Fatalf("second: %+v", second)
	}
	cancel()
	if !errors.Is(receive(t, done), context.Canceled) {
		t.Fatal("missing cancellation")
	}
}
func TestRunPeriodicTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan Sample[int])
	done := make(chan error, 1)
	go func() {
		done <- RunPeriodic(ctx, sourceFunc(func(c context.Context) (int, error) { <-c.Done(); return 9, nil }), Options{time.Second, time.Millisecond}, out)
	}()
	if s := receive(t, out); !errors.Is(s.Err, context.DeadlineExceeded) || s.Value != 0 {
		t.Fatalf("sample: %+v", s)
	}
	cancel()
	receive(t, done)
}
func TestRunPeriodicBlockedOutputCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	called := make(chan struct{})
	go func() {
		done <- RunPeriodic(ctx, sourceFunc(func(context.Context) (int, error) { close(called); return 1, nil }), DefaultOptions(), make(chan Sample[int]))
	}()
	receive(t, called)
	cancel()
	receive(t, done)
}
func TestRunPeriodicValidation(t *testing.T) {
	src := sourceFunc(func(context.Context) (int, error) { t.Error("unexpected call"); return 0, nil })
	out := make(chan Sample[int])
	for _, o := range []Options{{}, {time.Second, 0}, {0, time.Second}, {-1, time.Second}} {
		if RunPeriodic(context.Background(), src, o, out) == nil {
			t.Fatal("accepted options")
		}
	}
	if RunPeriodic(context.Background(), src, DefaultOptions(), nil) == nil {
		t.Fatal("accepted nil output")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(RunPeriodic(ctx, src, DefaultOptions(), out), context.Canceled) {
		t.Fatal("missing cancellation")
	}
}
func TestRunPeriodicNoOverlap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{}, 10)
	release := make(chan struct{})
	done := make(chan error, 1)
	out := make(chan Sample[int], 10)
	go func() {
		done <- RunPeriodic(ctx, sourceFunc(func(c context.Context) (int, error) {
			entered <- struct{}{}
			select {
			case <-release:
			case <-c.Done():
			}
			return 1, nil
		}), Options{10 * time.Millisecond, time.Second}, out)
	}()
	receive(t, entered)
	select {
	case <-entered:
		t.Fatal("overlapping collection")
	case <-time.After(35 * time.Millisecond):
	}
	close(release)
	receive(t, out)
	receive(t, entered)
	cancel()
	receive(t, done)
}

func TestRunPeriodicTimeoutPreservesSourceError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sentinel := errors.New("command failure")
	out := make(chan Sample[int])
	done := make(chan error, 1)
	go func() {
		done <- RunPeriodic(ctx, sourceFunc(func(c context.Context) (int, error) { <-c.Done(); return 1, sentinel }), Options{time.Second, time.Millisecond}, out)
	}()
	sample := receive(t, out)
	cancel()
	receive(t, done)
	if !errors.Is(sample.Err, context.DeadlineExceeded) || !errors.Is(sample.Err, sentinel) {
		t.Fatalf("lost error cause: %v", sample.Err)
	}
}

func TestRunPeriodicSkipsBusyTicks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{}, 10)
	release := make(chan struct{})
	out := make(chan Sample[int], 10)
	done := make(chan error, 1)
	go func() {
		done <- RunPeriodic(ctx, sourceFunc(func(c context.Context) (int, error) {
			entered <- struct{}{}
			select {
			case <-release:
			case <-c.Done():
			}
			return 1, nil
		}), Options{200 * time.Millisecond, time.Second}, out)
	}()
	receive(t, entered)
	time.Sleep(250 * time.Millisecond)
	close(release)
	receive(t, out)
	select {
	case <-entered:
		cancel()
		receive(t, done)
		t.Fatal("replayed a busy tick")
	case <-time.After(40 * time.Millisecond):
	}
	receive(t, entered)
	cancel()
	receive(t, done)
}
