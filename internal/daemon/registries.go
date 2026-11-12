package daemon

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/collector"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

var errMissing = errors.New("entity missing or unavailable")

type diskEntry struct {
	series   *aggregate.CounterSeries
	expected *aggregate.GaugeSeries[bool]
	seen     time.Time
}
type mountEntry struct {
	series   *aggregate.GaugeSeries[metrics.FilesystemUsage]
	expected *aggregate.GaugeSeries[bool]
	seen     time.Time
}

func (s *Store) addDisk(v collector.Sample[metrics.DiskIOObservation]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	at := v.FinishedAt
	seen := make(map[string]bool)
	partial := false
	success := 0
	s.details[2] = detail(v.Err)
	for key, e := range s.disks {
		if at.Sub(e.seen) > time.Duration(s.cfg.MaxWindowSeconds+2)*time.Second {
			delete(s.disks, key)
		}
	}
	if v.Err == nil {
		for _, r := range v.Value.Readings {
			if seen[r.Device] {
				return errors.New("duplicate disk key")
			}
			seen[r.Device] = true
			e := s.disks[r.Device]
			if e == nil {
				if len(s.disks) == 128 {
					partial = true
					continue
				}
				series, _ := aggregate.NewCounterSeries(s.cfg.MaxWindowSeconds)
				expected, _ := aggregate.NewGaugeSeries[bool](s.cfg.MaxWindowSeconds)
				e = &diskEntry{series: series, expected: expected}
				s.disks[r.Device] = e
			}
			e.seen = at
			if err := e.expected.Add(aggregate.Event[bool]{At: at, Value: true}); err != nil {
				return err
			}
			if err := e.series.Add(aggregate.Event[aggregate.CounterPoint]{At: at, Value: aggregate.DiskIOCounterPoint(r.Counters), Err: r.Err}); err != nil {
				return err
			}
			if r.Err == nil {
				success++
			} else {
				partial = true
				s.details[2] = detail(r.Err)
			}
		}
	}
	for key, e := range s.disks {
		if v.Err != nil || !seen[key] {
			if err := e.series.Add(aggregate.Event[aggregate.CounterPoint]{At: at, Err: errMissing}); err != nil {
				return err
			}
			if err := e.expected.Add(aggregate.Event[bool]{At: at, Err: errMissing}); err != nil {
				return err
			}
		}
	}
	healthErr := v.Err
	if healthErr == nil && len(v.Value.Readings) > 0 && success == 0 {
		healthErr = errMissing
	}
	if partial && s.details[2] == "" {
		s.details[2] = "disk registry capacity exceeded"
	}
	return s.diskHealth.Add(aggregate.Event[bool]{At: at, Value: len(v.Value.Readings) == 0, Err: healthErr, Partial: partial})
}
func (s *Store) addFilesystems(v collector.Sample[metrics.FilesystemObservation]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	at := v.FinishedAt
	seen := make(map[string]bool)
	partial := false
	success := 0
	s.details[3] = detail(v.Err)
	for key, e := range s.mounts {
		if at.Sub(e.seen) > time.Duration(s.cfg.MaxWindowSeconds+2)*time.Second {
			delete(s.mounts, key)
		}
	}
	if v.Err == nil {
		for _, r := range v.Value.Readings {
			if seen[r.MountPoint] {
				return errors.New("duplicate mount key")
			}
			seen[r.MountPoint] = true
			e := s.mounts[r.MountPoint]
			if e == nil {
				if len(s.mounts) == 128 {
					partial = true
					continue
				}
				series, _ := aggregate.NewGaugeSeries[metrics.FilesystemUsage](s.cfg.MaxWindowSeconds)
				expected, _ := aggregate.NewGaugeSeries[bool](s.cfg.MaxWindowSeconds)
				e = &mountEntry{series: series, expected: expected}
				s.mounts[r.MountPoint] = e
			}
			e.seen = at
			if err := e.expected.Add(aggregate.Event[bool]{At: at, Value: true}); err != nil {
				return err
			}
			if err := e.series.Add(aggregate.Event[metrics.FilesystemUsage]{At: at, Value: r.Usage, Err: r.Err}); err != nil {
				return err
			}
			if r.Err == nil {
				success++
			} else {
				partial = true
				s.details[3] = detail(r.Err)
			}
		}
	}
	for key, e := range s.mounts {
		if v.Err != nil || !seen[key] {
			if err := e.series.Add(aggregate.Event[metrics.FilesystemUsage]{At: at, Err: errMissing}); err != nil {
				return err
			}
			if err := e.expected.Add(aggregate.Event[bool]{At: at, Err: errMissing}); err != nil {
				return err
			}
		}
	}
	healthErr := v.Err
	if healthErr == nil && len(v.Value.Readings) > 0 && success == 0 {
		healthErr = errMissing
	}
	if partial && s.details[3] == "" {
		s.details[3] = "filesystem registry capacity exceeded"
	}
	return s.mountHealth.Add(aggregate.Event[bool]{At: at, Value: len(v.Value.Readings) == 0, Err: healthErr, Partial: partial})
}

type span struct{ start, end time.Time }

func spans[T any](w aggregate.Window, values []aggregate.Interval[T]) []span {
	out := make([]span, 0, len(values))
	for _, v := range values {
		a, b := v.Start, v.End
		if a.Before(w.Start) {
			a = w.Start
		}
		if b.After(w.End) {
			b = w.End
		}
		if a.Before(b) {
			out = append(out, span{a, b})
		}
	}
	return out
}

// Difference works on ordered nonoverlapping ranges and preserves that property.
func difference(a, b []span) []span {
	var out []span
	j := 0
	for _, v := range a {
		start := v.start
		for j < len(b) && !b[j].end.After(start) {
			j++
		}
		for k := j; k < len(b) && b[k].start.Before(v.end); k++ {
			if b[k].start.After(start) {
				out = append(out, span{start, b[k].start})
			}
			if b[k].end.After(start) {
				start = b[k].end
			}
			if !start.Before(v.end) {
				break
			}
		}
		if start.Before(v.end) {
			out = append(out, span{start, v.end})
		}
	}
	return out
}
func spanDuration(v []span) time.Duration {
	var d time.Duration
	for _, s := range v {
		d += s.end.Sub(s.start)
	}
	return d
}
func later(a, b *time.Time) *time.Time {
	if a == nil || b != nil && b.After(*a) {
		return b
	}
	return a
}
func registrySummary[T any](w aggregate.Window, health []aggregate.Interval[bool], good []span, data []T, last *time.Time, partial bool) aggregate.Summary[[]T] {
	if last == nil {
		return aggregate.Summary[[]T]{}
	}
	coverage := float64(spanDuration(good)) / float64(w.End.Sub(w.Start))
	return aggregate.Summary[[]T]{Data: &data, CoverageRatio: coverage, LastSampleAt: last, Partial: partial || coverage < 1}
}
func healthInfo(w aggregate.Window, v []aggregate.Interval[bool]) (bool, *time.Time, bool) {
	var empty bool
	var last *time.Time
	var partial bool
	for _, i := range v {
		if i.Value {
			empty = true
			t := i.ObservedAt
			last = later(last, &t)
		}
		partial = partial || i.Partial
	}
	return empty, last, partial
}
func (s *Store) diskSnapshot(ctx context.Context, w aggregate.Window) (aggregate.Summary[[]metrics.DiskIO], error) {
	health, err := s.diskHealth.Read(w)
	if err != nil {
		return aggregate.Summary[[]metrics.DiskIO]{}, err
	}
	good := spans(w, health)
	_, last, partial := healthInfo(w, health)
	var data []metrics.DiskIO
	for key, e := range s.disks {
		if err := ctx.Err(); err != nil {
			return aggregate.Summary[[]metrics.DiskIO]{}, err
		}
		values, err := e.series.Read(w)
		if err != nil {
			return aggregate.Summary[[]metrics.DiskIO]{}, err
		}
		result, err := aggregate.MeanDiskIO(w, key, values)
		if err != nil {
			return aggregate.Summary[[]metrics.DiskIO]{}, err
		}
		expected, err := e.expected.Read(w)
		if err != nil {
			return aggregate.Summary[[]metrics.DiskIO]{}, err
		}
		good = difference(good, difference(spans(w, expected), spans(w, values)))
		if result.Data != nil {
			data = append(data, *result.Data)
			last = later(last, result.LastSampleAt)
		}

	}
	sort.Slice(data, func(i, j int) bool { return data[i].Device < data[j].Device })
	return registrySummary(w, health, good, data, last, partial), nil
}
func (s *Store) mountSnapshot(ctx context.Context, w aggregate.Window) (aggregate.Summary[[]metrics.FilesystemUsage], error) {
	health, err := s.mountHealth.Read(w)
	if err != nil {
		return aggregate.Summary[[]metrics.FilesystemUsage]{}, err
	}
	good := spans(w, health)
	_, last, partial := healthInfo(w, health)
	var data []metrics.FilesystemUsage
	for key, e := range s.mounts {
		if err := ctx.Err(); err != nil {
			return aggregate.Summary[[]metrics.FilesystemUsage]{}, err
		}
		values, err := e.series.Read(w)
		if err != nil {
			return aggregate.Summary[[]metrics.FilesystemUsage]{}, err
		}
		result, err := aggregate.MeanFilesystemUsage(w, key, values)
		if err != nil {
			return aggregate.Summary[[]metrics.FilesystemUsage]{}, err
		}
		expected, err := e.expected.Read(w)
		if err != nil {
			return aggregate.Summary[[]metrics.FilesystemUsage]{}, err
		}
		usable := values
		if result.Data != nil {
			usable = nil
			for _, v := range values {
				if v.Value.Identity == result.Data.Identity {
					usable = append(usable, v)
				}
			}
		}
		good = difference(good, difference(spans(w, expected), spans(w, usable)))
		if result.Data != nil {
			for _, field := range []*float64{result.Data.UsedPercent, result.Data.UsedInodes, result.Data.UsedInodesPercent} {
				if field != nil {
					var fieldValues []aggregate.Interval[metrics.FilesystemUsage]
					for _, v := range usable {
						switch field {
						case result.Data.UsedPercent:
							if v.Value.UsedPercent != nil {
								fieldValues = append(fieldValues, v)
							}
						case result.Data.UsedInodes:
							if v.Value.UsedInodes != nil {
								fieldValues = append(fieldValues, v)
							}
						case result.Data.UsedInodesPercent:
							if v.Value.UsedInodesPercent != nil {
								fieldValues = append(fieldValues, v)
							}
						}
					}
					if spanDuration(spans(w, fieldValues)) < spanDuration(spans(w, usable)) {
						partial = true
					}
				}
			}
		}
		if result.Data != nil {
			data = append(data, *result.Data)
			last = later(last, result.LastSampleAt)
		}

	}
	sort.Slice(data, func(i, j int) bool { return data[i].MountPoint < data[j].MountPoint })
	return registrySummary(w, health, good, data, last, partial), nil
}
