package daemon

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/collector"
	"github.com/kefiriaus/otus-go-final-project/internal/config"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

type sourceFunc[T any] func(context.Context) (T, error)

func (f sourceFunc[T]) Collect(ctx context.Context) (T, error) { return f(ctx) }
func TestRuntimeCollectsWithoutSubscribers(t *testing.T) {
	cfg := config.Default()
	cfg.Subsystems = config.Subsystems{LoadAverage: true, CPU: true}
	var calls atomic.Int32
	src := sources{
		load: sourceFunc[metrics.LoadAverage](func(context.Context) (metrics.LoadAverage, error) {
			calls.Add(1)
			return metrics.LoadAverage{Load1: 7}, nil
		}),
		cpu: sourceFunc[metrics.CPUCounters](
			func(context.Context) (metrics.CPUCounters, error) { return metrics.CPUCounters{}, errors.New("denied") },
		),
	}
	r, err := newRuntime(cfg, src, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for {
		now := time.Now()
		snap, err := r.Snapshot(ctx, aggregate.Window{Start: now.Add(-time.Second), End: now})
		if err != nil {
			t.Fatal(err)
		}
		if snap.LoadAverage.Summary.Data != nil && snap.CPU.Detail != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no collection without subscribers")
		}
		time.Sleep(time.Millisecond)
	}
	if calls.Load() == 0 {
		t.Fatal("source not started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers did not stop")
	}
}

func TestDisabledRuntimeDoesNotStartSources(t *testing.T) {
	cfg := config.Default()
	cfg.Subsystems = config.Subsystems{}
	r, _ := newRuntime(
		cfg,
		sources{
			load: sourceFunc[metrics.LoadAverage](
				func(context.Context) (metrics.LoadAverage, error) {
					t.Error("disabled source called")
					return metrics.LoadAverage{}, nil
				},
			),
		},
		time.Now,
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	cancel()
	<-done
}

func TestTrafficCheckpointCoalescesStaleHeartbeat(t *testing.T) {
	cfg := config.Default()
	cfg.MaxWindowSeconds = 10
	s, _ := NewStore(cfg)
	at := time.Unix(100, 0).UTC()
	s.now = func() time.Time { return at.Add(2 * time.Second) }
	s.traffic, _ = aggregate.NewTrafficHistory(
		10,
		[]string{"eth0"},
		aggregate.DefaultTrafficHistoryOptions(),
	)
	sink := trafficSink{s}
	if err := sink.SetInterfaceState("eth0", at, true); err != nil {
		t.Fatal(err)
	}
	s.Snapshot(context.Background(), aggregate.Window{Start: at, End: at.Add(2 * time.Second)})
	if err := sink.Advance(at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Snapshot(
		context.Background(),
		aggregate.Window{Start: at, End: at.Add(2 * time.Second)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.Traffic.Summary.CoverageRatio != 1 || got.Traffic.Summary.Data == nil {
		t.Fatalf("%+v", got.Traffic)
	}
}

var _ collector.Source[metrics.LoadAverage] = sourceFunc[metrics.LoadAverage](nil)

func TestProductionDisabledSourcesAreNotConstructed(t *testing.T) {
	cfg := config.Default()
	cfg.Subsystems = config.Subsystems{}
	r, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.sources.load != nil || r.sources.cpu != nil || r.sources.disk != nil ||
		r.sources.filesystems != nil ||
		r.sources.connections != nil ||
		r.sources.traffic != nil {
		t.Fatal("disabled sources constructed")
	}
}

func TestConcurrentTrafficCallbacksAndSnapshots(t *testing.T) {
	cfg := config.Default()
	cfg.MaxWindowSeconds = 10
	s, _ := NewStore(cfg)
	at := time.Unix(100, 0).UTC()
	s.now = func() time.Time { return at.Add(2 * time.Second) }
	s.traffic, _ = aggregate.NewTrafficHistory(
		10,
		[]string{"eth0"},
		aggregate.DefaultTrafficHistoryOptions(),
	)
	sink := trafficSink{s}
	sink.SetInterfaceState("eth0", at, true)
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		for i := 0; i < 100; i++ {
			if err := sink.Advance(at.Add(time.Second)); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		defer group.Done()
		for i := 0; i < 100; i++ {
			got, err := s.Snapshot(
				context.Background(),
				aggregate.Window{Start: at, End: at.Add(2 * time.Second)},
			)
			if err != nil {
				t.Error(err)
			} else if got.Traffic.Summary.CoverageRatio != 1 {
				t.Error("checkpoint gap")
			}
		}
	}()
	group.Wait()
}
