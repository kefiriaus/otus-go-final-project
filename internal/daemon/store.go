package daemon

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/collector"
	"github.com/kefiriaus/otus-go-final-project/internal/config"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

type Store struct {
	mu                      sync.Mutex
	cfg                     config.Config
	now                     func() time.Time
	load                    *aggregate.GaugeSeries[metrics.LoadAverage]
	cpu                     *aggregate.CounterSeries
	disks                   map[string]*diskEntry
	mounts                  map[string]*mountEntry
	diskHealth, mountHealth *aggregate.GaugeSeries[bool]
	connections             connectionHistory
	traffic                 *aggregate.TrafficHistory
	trafficWatermark        time.Time
	details                 [6]string
}

func NewStore(cfg config.Config) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	load, _ := aggregate.NewGaugeSeries[metrics.LoadAverage](cfg.MaxWindowSeconds)
	cpu, _ := aggregate.NewCounterSeries(cfg.MaxWindowSeconds)
	dh, _ := aggregate.NewGaugeSeries[bool](cfg.MaxWindowSeconds)
	mh, _ := aggregate.NewGaugeSeries[bool](cfg.MaxWindowSeconds)
	ch, _ := aggregate.NewGaugeSeries[metrics.Connections](cfg.MaxWindowSeconds)
	return &Store{
		cfg:         cfg,
		now:         time.Now,
		load:        load,
		cpu:         cpu,
		disks:       make(map[string]*diskEntry),
		mounts:      make(map[string]*mountEntry),
		diskHealth:  dh,
		mountHealth: mh,
		connections: connectionHistory{
			series:     ch,
			limit:      32 << 20,
			maxEntries: cfg.MaxWindowSeconds + 2,
			retention:  time.Duration(cfg.MaxWindowSeconds+2) * time.Second,
		},
	}, nil
}

func detail(err error) string {
	if err == nil {
		return ""
	}
	s := strings.ToValidUTF8(err.Error(), "?")
	if len(s) > 512 {
		s = strings.ToValidUTF8(s[:509], "?") + "..."
	}
	return s
}

func (s *Store) addLoad(v collector.Sample[metrics.LoadAverage]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.details[0] = detail(v.Err)
	return s.load.Add(
		aggregate.Event[metrics.LoadAverage]{At: v.FinishedAt, Value: v.Value, Err: v.Err},
	)
}

func (s *Store) addCPU(v collector.Sample[metrics.CPUCounters]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.details[1] = detail(v.Err)
	return s.cpu.Add(
		aggregate.Event[aggregate.CounterPoint]{
			At:    v.FinishedAt,
			Value: aggregate.CPUCounterPoint(v.Value),
			Err:   v.Err,
		},
	)
}

func (s *Store) addConnections(v collector.Sample[metrics.ConnectionsObservation]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.details[5] = detail(v.Err)
	if v.Err == nil && v.Value.Detail != "" {
		s.details[5] = detail(fmt.Errorf("%s", v.Value.Detail))
	}
	err := s.connections.add(v)
	if s.connections.diagnostic != "" {
		s.details[5] = s.connections.diagnostic
	}
	return err
}

func (s *Store) Snapshot(ctx context.Context, w aggregate.Window) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if w.Start.IsZero() || !w.End.After(w.Start) ||
		w.End.Sub(w.Start) > time.Duration(s.cfg.MaxWindowSeconds)*time.Second {
		return Snapshot{}, fmt.Errorf("invalid snapshot window")
	}
	out := Snapshot{
		Window:      w,
		GeneratedAt: s.now().UTC(),
		LoadAverage: Section[metrics.LoadAverage]{
			Enabled: s.cfg.Subsystems.LoadAverage,
			Detail:  s.details[0],
		},
		CPU: Section[metrics.CPUUsage]{Enabled: s.cfg.Subsystems.CPU, Detail: s.details[1]},
		DiskIO: Section[[]metrics.DiskIO]{
			Enabled: s.cfg.Subsystems.DiskIO,
			Detail:  s.details[2],
		},
		Filesystems: Section[[]metrics.FilesystemUsage]{
			Enabled: s.cfg.Subsystems.Filesystems,
			Detail:  s.details[3],
		},
		Traffic: Section[metrics.Traffic]{
			Enabled: s.cfg.Subsystems.Traffic,
			Detail:  s.details[4],
		},
		Connections: Section[metrics.Connections]{
			Enabled: s.cfg.Subsystems.Connections,
			Detail:  s.details[5],
		},
	}
	var err error
	if out.LoadAverage.Enabled {
		v, e := s.load.Read(w)
		if e != nil {
			return Snapshot{}, e
		}
		out.LoadAverage.Summary, err = aggregate.MeanLoadAverage(w, v)
		if err != nil {
			return Snapshot{}, err
		}
	}
	if out.CPU.Enabled {
		v, e := s.cpu.Read(w)
		if e != nil {
			return Snapshot{}, e
		}
		out.CPU.Summary, err = aggregate.MeanCPUUsage(w, v)
		if err != nil {
			return Snapshot{}, err
		}
	}
	if out.DiskIO.Enabled {
		out.DiskIO.Summary, err = s.diskSnapshot(ctx, w)
		if err != nil {
			return Snapshot{}, err
		}
	}
	if out.Filesystems.Enabled {
		out.Filesystems.Summary, err = s.mountSnapshot(ctx, w)
		if err != nil {
			return Snapshot{}, err
		}
	}
	if out.Connections.Enabled {
		v, e := s.connections.series.Read(w)
		if e != nil {
			return Snapshot{}, e
		}
		out.Connections.Summary, err = aggregate.LatestConnections(w, v)
		if err != nil {
			return Snapshot{}, err
		}
	}
	if out.Traffic.Enabled && s.traffic != nil {
		if err = s.advanceLocked(s.now().UTC()); err != nil {
			return Snapshot{}, err
		}
		out.Traffic.Summary, err = s.traffic.Snapshot(w, 20)
		if err != nil {
			return Snapshot{}, err
		}
	}
	return out, ctx.Err()
}
