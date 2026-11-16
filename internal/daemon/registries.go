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

type registryEntry interface {
	seenAt() time.Time
	addMissing(time.Time) error
}

func (e *diskEntry) seenAt() time.Time  { return e.seen }
func (e *mountEntry) seenAt() time.Time { return e.seen }

func (e *diskEntry) addMissing(at time.Time) error {
	if err := e.series.Add(aggregate.Event[aggregate.CounterPoint]{At: at, Err: errMissing}); err != nil {
		return err
	}
	return e.expected.Add(aggregate.Event[bool]{At: at, Err: errMissing})
}

func (e *mountEntry) addMissing(at time.Time) error {
	if err := e.series.Add(aggregate.Event[metrics.FilesystemUsage]{At: at, Err: errMissing}); err != nil {
		return err
	}
	return e.expected.Add(aggregate.Event[bool]{At: at, Err: errMissing})
}

func updateRegistry[T registryEntry](
	at time.Time,
	sampleErr error,
	readingCount int,
	retention time.Duration,
	entries map[string]T,
	ingest func(time.Time) (map[string]bool, int, bool, error),
	health *aggregate.GaugeSeries[bool],
) (bool, error) {
	for key, e := range entries {
		if at.Sub(e.seenAt()) > retention {
			delete(entries, key)
		}
	}
	seen, success, partial, err := ingest(at)
	if err != nil {
		return false, err
	}
	for key, e := range entries {
		if sampleErr != nil || !seen[key] {
			if err := e.addMissing(at); err != nil {
				return false, err
			}
		}
	}
	healthErr := sampleErr
	if healthErr == nil && readingCount > 0 && success == 0 {
		healthErr = errMissing
	}
	return partial, health.Add(aggregate.Event[bool]{
		At: at, Value: readingCount == 0, Err: healthErr, Partial: partial,
	})
}

func addRegistrySample[T registryEntry, R any](
	s *Store,
	at time.Time,
	sampleErr error,
	readings []R,
	readingCount int,
	entries map[string]T,
	ingest func(time.Time, []R) (map[string]bool, int, bool, error),
	health *aggregate.GaugeSeries[bool],
	detailIndex int,
	capacityMessage string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.details[detailIndex] = detail(sampleErr)
	if sampleErr != nil {
		readings = nil
	}
	ingestAt := func(at time.Time) (map[string]bool, int, bool, error) {
		return ingest(at, readings)
	}
	retention := time.Duration(s.cfg.MaxWindowSeconds+2) * time.Second
	partial, err := updateRegistry(at, sampleErr, readingCount, retention, entries, ingestAt, health)
	if partial && s.details[detailIndex] == "" {
		s.details[detailIndex] = capacityMessage
	}
	return err
}

func (s *Store) diskEntryAt(at time.Time, key string) (*diskEntry, bool) {
	e := s.disks[key]
	if e == nil {
		if len(s.disks) == 128 {
			return nil, false
		}
		series, _ := aggregate.NewCounterSeries(s.cfg.MaxWindowSeconds)
		expected, _ := aggregate.NewGaugeSeries[bool](s.cfg.MaxWindowSeconds)
		e = &diskEntry{series: series, expected: expected}
		s.disks[key] = e
	}
	e.seen = at
	return e, true
}

func (s *Store) addDiskReadings(
	at time.Time,
	readings []metrics.DiskIOReading,
) (map[string]bool, int, bool, error) {
	seen := make(map[string]bool)
	success := 0
	partial := false
	for _, r := range readings {
		if seen[r.Device] {
			return nil, 0, false, errors.New("duplicate disk key")
		}
		seen[r.Device] = true
		e, retained := s.diskEntryAt(at, r.Device)
		if !retained {
			partial = true
			continue
		}
		if err := e.expected.Add(aggregate.Event[bool]{At: at, Value: true}); err != nil {
			return nil, 0, false, err
		}
		if err := e.series.Add(aggregate.Event[aggregate.CounterPoint]{
			At: at, Value: aggregate.DiskIOCounterPoint(r.Counters), Err: r.Err,
		}); err != nil {
			return nil, 0, false, err
		}
		if r.Err == nil {
			success++
		} else {
			partial = true
			s.details[2] = detail(r.Err)
		}
	}
	return seen, success, partial, nil
}

func (s *Store) addDisk(v collector.Sample[metrics.DiskIOObservation]) error {
	return addRegistrySample(
		s, v.FinishedAt, v.Err, v.Value.Readings, len(v.Value.Readings), s.disks,
		s.addDiskReadings, s.diskHealth, 2, "disk registry capacity exceeded",
	)
}

func (s *Store) mountEntryAt(at time.Time, key string) (*mountEntry, bool) {
	e := s.mounts[key]
	if e == nil {
		if len(s.mounts) == 128 {
			return nil, false
		}
		series, _ := aggregate.NewGaugeSeries[metrics.FilesystemUsage](s.cfg.MaxWindowSeconds)
		expected, _ := aggregate.NewGaugeSeries[bool](s.cfg.MaxWindowSeconds)
		e = &mountEntry{series: series, expected: expected}
		s.mounts[key] = e
	}
	e.seen = at
	return e, true
}

func (s *Store) addFilesystemReadings(
	at time.Time,
	readings []metrics.FilesystemReading,
) (map[string]bool, int, bool, error) {
	seen := make(map[string]bool)
	success := 0
	partial := false
	for _, r := range readings {
		if seen[r.MountPoint] {
			return nil, 0, false, errors.New("duplicate mount key")
		}
		seen[r.MountPoint] = true
		e, retained := s.mountEntryAt(at, r.MountPoint)
		if !retained {
			partial = true
			continue
		}
		if err := e.expected.Add(aggregate.Event[bool]{At: at, Value: true}); err != nil {
			return nil, 0, false, err
		}
		if err := e.series.Add(aggregate.Event[metrics.FilesystemUsage]{
			At: at, Value: r.Usage, Err: r.Err,
		}); err != nil {
			return nil, 0, false, err
		}
		if r.Err == nil {
			success++
		} else {
			partial = true
			s.details[3] = detail(r.Err)
		}
	}
	return seen, success, partial, nil
}

func (s *Store) addFilesystems(v collector.Sample[metrics.FilesystemObservation]) error {
	return addRegistrySample(
		s, v.FinishedAt, v.Err, v.Value.Readings, len(v.Value.Readings), s.mounts,
		s.addFilesystemReadings, s.mountHealth, 3, "filesystem registry capacity exceeded",
	)
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

func registrySummary[T any](
	w aggregate.Window,
	good []span,
	data []T,
	last *time.Time,
	partial bool,
) aggregate.Summary[[]T] {
	if last == nil {
		return aggregate.Summary[[]T]{}
	}
	coverage := float64(spanDuration(good)) / float64(w.End.Sub(w.Start))
	return aggregate.Summary[[]T]{
		Data:          &data,
		CoverageRatio: coverage,
		LastSampleAt:  last,
		Partial:       partial || coverage < 1,
	}
}

func healthInfo(v []aggregate.Interval[bool]) (*time.Time, bool) {
	var last *time.Time
	var partial bool
	for _, i := range v {
		if i.Value {
			t := i.ObservedAt
			last = later(last, &t)
		}
		partial = partial || i.Partial
	}
	return last, partial
}

func (s *Store) diskSnapshot(
	ctx context.Context,
	w aggregate.Window,
) (aggregate.Summary[[]metrics.DiskIO], error) {
	health, err := s.diskHealth.Read(w)
	if err != nil {
		return aggregate.Summary[[]metrics.DiskIO]{}, err
	}
	good := spans(w, health)
	last, partial := healthInfo(health)
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
	return registrySummary(w, good, data, last, partial), nil
}

func filesystemField(value metrics.FilesystemUsage, field int) *float64 {
	switch field {
	case 0:
		return value.UsedPercent
	case 1:
		return value.UsedInodes
	default:
		return value.UsedInodesPercent
	}
}

func mountFieldsPartial(
	w aggregate.Window,
	usable []aggregate.Interval[metrics.FilesystemUsage],
	result *metrics.FilesystemUsage,
) bool {
	for field := 0; field < 3; field++ {
		if filesystemField(*result, field) == nil {
			continue
		}
		var fieldValues []aggregate.Interval[metrics.FilesystemUsage]
		for _, v := range usable {
			if filesystemField(v.Value, field) != nil {
				fieldValues = append(fieldValues, v)
			}
		}
		if spanDuration(spans(w, fieldValues)) < spanDuration(spans(w, usable)) {
			return true
		}
	}
	return false
}

func mountSnapshotPart(
	w aggregate.Window,
	key string,
	e *mountEntry,
) (aggregate.Summary[metrics.FilesystemUsage], []span, bool, error) {
	values, err := e.series.Read(w)
	if err != nil {
		return aggregate.Summary[metrics.FilesystemUsage]{}, nil, false, err
	}
	expected, err := e.expected.Read(w)
	if err != nil {
		return aggregate.Summary[metrics.FilesystemUsage]{}, nil, false, err
	}
	result, err := aggregate.MeanFilesystemUsage(w, key, values)
	if err != nil {
		return aggregate.Summary[metrics.FilesystemUsage]{}, nil, false, err
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
	missing := difference(spans(w, expected), spans(w, usable))
	if result.Data == nil {
		return aggregate.Summary[metrics.FilesystemUsage]{}, missing, false, nil
	}
	return result, missing, mountFieldsPartial(w, usable, result.Data), nil
}

func (s *Store) mountSnapshot(
	ctx context.Context,
	w aggregate.Window,
) (aggregate.Summary[[]metrics.FilesystemUsage], error) {
	health, err := s.mountHealth.Read(w)
	if err != nil {
		return aggregate.Summary[[]metrics.FilesystemUsage]{}, err
	}
	good := spans(w, health)
	last, partial := healthInfo(health)
	var data []metrics.FilesystemUsage
	for key, e := range s.mounts {
		if err := ctx.Err(); err != nil {
			return aggregate.Summary[[]metrics.FilesystemUsage]{}, err
		}
		usage, missing, fieldPartial, err := mountSnapshotPart(w, key, e)
		if err != nil {
			return aggregate.Summary[[]metrics.FilesystemUsage]{}, err
		}
		good = difference(good, missing)
		partial = partial || fieldPartial
		if usage.Data != nil {
			data = append(data, *usage.Data)
			last = later(last, usage.LastSampleAt)
		}
	}
	sort.Slice(data, func(i, j int) bool { return data[i].MountPoint < data[j].MountPoint })
	return registrySummary(w, good, data, last, partial), nil
}
