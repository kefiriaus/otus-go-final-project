package daemon

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/collector"
	"github.com/kefiriaus/otus-go-final-project/internal/config"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

type capture interface {
	Interfaces() []string
	Run(context.Context, collector.TrafficSink) error
	LastError() error
}
type sources struct {
	load        collector.Source[metrics.LoadAverage]
	cpu         collector.Source[metrics.CPUCounters]
	disk        collector.Source[metrics.DiskIOObservation]
	filesystems collector.Source[metrics.FilesystemObservation]
	connections collector.Source[metrics.ConnectionsObservation]
	traffic     capture
	errors      [6]error
}

type Runtime struct {
	store   *Store
	sources sources
	started atomic.Bool
}

func NewRuntime(cfg config.Config) (*Runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newRuntime(cfg, platformSources(cfg), time.Now)
}
func newRuntime(cfg config.Config, src sources, now func() time.Time) (*Runtime, error) {
	s, err := NewStore(cfg)
	if err != nil {
		return nil, err
	}
	s.now = now
	for i, e := range src.errors {
		s.details[i] = detail(e)
	}
	if cfg.Subsystems.Traffic && src.traffic != nil {
		s.traffic, err = aggregate.NewTrafficHistory(cfg.MaxWindowSeconds, src.traffic.Interfaces(), aggregate.DefaultTrafficHistoryOptions())
		if err != nil {
			s.details[4] = detail(err)
			src.traffic = nil
		}
	}
	return &Runtime{store: s, sources: src}, nil
}
func (r *Runtime) Snapshot(ctx context.Context, w aggregate.Window) (Snapshot, error) {
	if r.sources.traffic != nil {
		e := r.sources.traffic.LastError()
		r.store.mu.Lock()
		r.store.details[4] = detail(e)
		r.store.mu.Unlock()
	}
	return r.store.Snapshot(ctx, w)
}
func periodic[T any](src collector.Source[T], add func(collector.Sample[T]) error) []collector.Worker {
	samples := make(chan collector.Sample[T], 1)
	return []collector.Worker{
		func(ctx context.Context) error {
			return collector.RunPeriodic(ctx, src, collector.DefaultOptions(), samples)
		},
		func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case v := <-samples:
					if err := add(v); err != nil {
						return err
					}
				}
			}
		},
	}
}
func (r *Runtime) Run(ctx context.Context) error {
	if !r.started.CompareAndSwap(false, true) {
		return fmt.Errorf("runtime already started")
	}
	cfg := r.store.cfg.Subsystems
	src := r.sources
	var workers []collector.Worker
	if cfg.LoadAverage && src.load != nil {
		workers = append(workers, periodic(src.load, r.store.addLoad)...)
	}
	if cfg.CPU && src.cpu != nil {
		workers = append(workers, periodic(src.cpu, r.store.addCPU)...)
	}
	if cfg.DiskIO && src.disk != nil {
		workers = append(workers, periodic(src.disk, r.store.addDisk)...)
	}
	if cfg.Filesystems && src.filesystems != nil {
		workers = append(workers, periodic(src.filesystems, r.store.addFilesystems)...)
	}
	if cfg.Connections && src.connections != nil {
		workers = append(workers, periodic(src.connections, r.store.addConnections)...)
	}
	if cfg.Traffic && src.traffic != nil {
		workers = append(workers, func(ctx context.Context) error { return src.traffic.Run(ctx, trafficSink{r.store}) })
	}
	return collector.RunGroup(ctx, workers...)
}
