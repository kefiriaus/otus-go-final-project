package daemon

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/collector"
	"github.com/kefiriaus/otus-go-final-project/internal/config"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const (
	testFilesystemType  = "ext4"
	testOriginalCommand = "original"
)

func TestStoreCoverageAndIsolation(t *testing.T) {
	cfg := config.Default()
	cfg.MaxWindowSeconds = 10
	s, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(100, 0).UTC()
	for i := 4; i < 10; i++ {
		if err := s.addLoad(
			collector.Sample[metrics.LoadAverage]{
				FinishedAt: at.Add(time.Duration(i) * time.Second),
				Value:      metrics.LoadAverage{Load1: 2, Load5: 3, Load15: 4},
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	s.addCPU(collector.Sample[metrics.CPUCounters]{FinishedAt: at, Err: errors.New("unavailable")})
	w := aggregate.Window{Start: at, End: at.Add(10 * time.Second)}
	a, err := s.Snapshot(context.Background(), w)
	if err != nil {
		t.Fatal(err)
	}
	if a.LoadAverage.Summary.CoverageRatio != 0.6 ||
		math.Abs(a.LoadAverage.Summary.Data.Load1-2) > 1e-12 ||
		a.CPU.Summary.Data != nil {
		t.Fatalf("%+v", a)
	}
	a.LoadAverage.Summary.Data.Load1 = 99
	b, _ := s.Snapshot(context.Background(), w)
	if math.Abs(b.LoadAverage.Summary.Data.Load1-2) > 1e-12 {
		t.Fatal("snapshot alias")
	}
}

func TestRegistryEmptyFailedAndBounded(t *testing.T) {
	s, _ := NewStore(config.Default())
	at := time.Unix(100, 0).UTC()
	s.addDisk(collector.Sample[metrics.DiskIOObservation]{FinishedAt: at})
	empty, err := s.Snapshot(
		context.Background(),
		aggregate.Window{Start: at, End: at.Add(time.Second)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if empty.DiskIO.Summary.Data == nil || empty.DiskIO.Summary.CoverageRatio != 1 {
		t.Fatal("empty enumeration unavailable")
	}
	for i := 1; i <= 3600; i++ {
		reading := metrics.DiskIOReading{
			Device:   fmt.Sprint(i),
			Counters: metrics.DiskIOCounters{Identity: fmt.Sprint(i)},
		}
		if err := s.addDisk(
			collector.Sample[metrics.DiskIOObservation]{
				FinishedAt: at.Add(time.Duration(i) * time.Second),
				Value:      metrics.DiskIOObservation{Readings: []metrics.DiskIOReading{reading}},
			},
		); err != nil {
			t.Fatal(err)
		}
		if len(s.disks) > 128 {
			t.Fatal("registry grew")
		}
	}
	s2, _ := NewStore(config.Default())
	s2.addFilesystems(
		collector.Sample[metrics.FilesystemObservation]{
			FinishedAt: at,
			Value: metrics.FilesystemObservation{
				Readings: []metrics.FilesystemReading{{MountPoint: "/", Err: errors.New("denied")}},
			},
		},
	)
	failed, err := s2.Snapshot(
		context.Background(),
		aggregate.Window{Start: at, End: at.Add(time.Second)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Filesystems.Summary.Data != nil {
		t.Fatal("all failed reported usable")
	}
}

func TestConnectionBudget(t *testing.T) {
	s, _ := NewStore(config.Default())
	s.connections.limit = 600
	at := time.Unix(100, 0).UTC()
	for i := 0; i < 10; i++ {
		if err := s.addConnections(
			collector.Sample[metrics.ConnectionsObservation]{
				FinishedAt: at.Add(time.Duration(i) * time.Second),
				Value: metrics.ConnectionsObservation{
					Data: metrics.Connections{
						TCPStates: []metrics.TCPStateCount{
							{State: "ESTABLISHED", Count: uint64(i)},
						},
					},
				},
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Snapshot(
		context.Background(),
		aggregate.Window{Start: at, End: at.Add(10 * time.Second)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.Connections.Summary.Data == nil || got.Connections.Summary.CoverageRatio >= 1 ||
		s.connections.bytes > s.connections.limit {
		t.Fatalf("budget/coverage: %+v bytes %d", got.Connections, s.connections.bytes)
	}
}

func TestFilesystemBirthDoesNotReduceSectionCoverage(t *testing.T) {
	s, _ := NewStore(config.Default())
	at := time.Unix(100, 0).UTC()
	s.addFilesystems(collector.Sample[metrics.FilesystemObservation]{FinishedAt: at})
	usage := metrics.FilesystemUsage{
		MountPoint:     "/",
		Identity:       "a",
		Device:         "/dev/a",
		FilesystemType: testFilesystemType,
		UsedMebibytes:  10,
	}
	s.addFilesystems(
		collector.Sample[metrics.FilesystemObservation]{
			FinishedAt: at.Add(time.Second),
			Value: metrics.FilesystemObservation{
				Readings: []metrics.FilesystemReading{{MountPoint: "/", Usage: usage}},
			},
		},
	)
	got, err := s.Snapshot(
		context.Background(),
		aggregate.Window{Start: at, End: at.Add(2 * time.Second)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.Filesystems.Summary.Data == nil || got.Filesystems.Summary.CoverageRatio != 1 ||
		got.Filesystems.Summary.Partial {
		t.Fatalf("new mount penalized: %+v", got.Filesystems.Summary)
	}
}

func TestFilesystemFailureAndRemount(t *testing.T) {
	s, _ := NewStore(config.Default())
	at := time.Unix(100, 0).UTC()
	a := metrics.FilesystemUsage{
		MountPoint:     "/a",
		Identity:       "one",
		Device:         "/dev/a",
		FilesystemType: testFilesystemType,
		UsedMebibytes:  10,
	}
	b := metrics.FilesystemUsage{
		MountPoint:     "/b",
		Identity:       "two",
		Device:         "/dev/b",
		FilesystemType: testFilesystemType,
		UsedMebibytes:  20,
	}
	for i := 0; i < 3; i++ {
		readings := []metrics.FilesystemReading{
			{MountPoint: "/a", Usage: a},
			{MountPoint: "/b", Usage: b},
		}
		if i == 1 {
			readings[1].Err = errors.New("denied")
		}
		if i == 2 {
			readings[0].Usage.Identity = "new"
			readings[0].Usage.UsedMebibytes = 50
		}
		if err := s.addFilesystems(
			collector.Sample[metrics.FilesystemObservation]{
				FinishedAt: at.Add(time.Duration(i) * time.Second),
				Value:      metrics.FilesystemObservation{Readings: readings},
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Snapshot(
		context.Background(),
		aggregate.Window{Start: at, End: at.Add(3 * time.Second)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.Filesystems.Summary.Data == nil || !got.Filesystems.Summary.Partial ||
		got.Filesystems.Summary.CoverageRatio >= 1 {
		t.Fatalf("%+v", got.Filesystems.Summary)
	}
	if (*got.Filesystems.Summary.Data)[0].UsedMebibytes != 50 {
		t.Fatal("remount combined identities")
	}
}

func TestConnectionsBudgetRejectsOversizedAndExpiresAfterGap(t *testing.T) {
	cfg := config.Default()
	cfg.MaxWindowSeconds = 10
	s, _ := NewStore(cfg)
	at := time.Unix(100, 0).UTC()
	for i := 0; i < 3; i++ {
		if err := s.addConnections(
			collector.Sample[metrics.ConnectionsObservation]{
				FinishedAt: at.Add(time.Duration(i) * time.Second),
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.addConnections(
		collector.Sample[metrics.ConnectionsObservation]{FinishedAt: at.Add(time.Hour)},
	); err != nil {
		t.Fatal(err)
	}
	if len(s.connections.charges) != 1 {
		t.Fatalf("expired charges retained: %d", len(s.connections.charges))
	}
	s.connections.limit = 1
	if err := s.addConnections(
		collector.Sample[metrics.ConnectionsObservation]{
			FinishedAt: at.Add(time.Hour + time.Second),
		},
	); err != nil {
		t.Fatal(err)
	}
	got, err := s.Snapshot(
		context.Background(),
		aggregate.Window{
			Start: at.Add(time.Hour + time.Second),
			End:   at.Add(time.Hour + 2*time.Second),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.Connections.Summary.Data != nil || got.Connections.Detail == "" {
		t.Fatalf("oversized sample not unavailable with diagnostic: %+v", got.Connections)
	}
}

func TestDiskMissingAndGenerationRecovery(t *testing.T) {
	s, _ := NewStore(config.Default())
	at := time.Unix(100, 0).UTC()
	for i := 0; i < 6; i++ {
		observation := metrics.DiskIOObservation{}
		if i != 2 {
			identity := "old"
			if i >= 4 {
				identity = "new"
			}
			observation.Readings = []metrics.DiskIOReading{
				{
					Device: "sda",
					Counters: metrics.DiskIOCounters{
						Identity:       identity,
						ReadsCompleted: uint64(i * 10),
					},
				},
			}
		}
		if err := s.addDisk(
			collector.Sample[metrics.DiskIOObservation]{
				FinishedAt: at.Add(time.Duration(i) * time.Second),
				Value:      observation,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	intervals, err := s.disks["sda"].series.Read(
		aggregate.Window{Start: at, End: at.Add(6 * time.Second)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(intervals) != 2 || intervals[0].Start != at ||
		intervals[1].Start != at.Add(4*time.Second) {
		t.Fatalf("bridged disappearance or generation change: %+v", intervals)
	}
}

func TestConnectionStoreOwnsNestedValues(t *testing.T) {
	s, _ := NewStore(config.Default())
	at := time.Unix(100, 0).UTC()
	command := testOriginalCommand
	pid := uint32(12)
	value := metrics.Connections{
		ListeningSockets: []metrics.ListeningSocket{
			{
				Local: metrics.Endpoint{
					Address: netip.MustParseAddr("127.0.0.1"),
					HasPort: true,
				},
				Protocol: 6,
				Owners:   []metrics.ProcessOwner{{Command: &command, PID: &pid}},
			},
		},
	}
	if err := s.addConnections(
		collector.Sample[metrics.ConnectionsObservation]{
			FinishedAt: at,
			Value:      metrics.ConnectionsObservation{Data: value},
		},
	); err != nil {
		t.Fatal(err)
	}
	command = "mutated"
	pid = 99
	w := aggregate.Window{Start: at, End: at.Add(time.Second)}
	a, err := s.Snapshot(context.Background(), w)
	if err != nil {
		t.Fatal(err)
	}
	owner := a.Connections.Summary.Data.ListeningSockets[0].Owners[0]
	if *owner.Command != testOriginalCommand || *owner.PID != 12 {
		t.Fatal("input aliases retained history")
	}
	*owner.Command = "output mutation"
	b, _ := s.Snapshot(context.Background(), w)
	if *b.Connections.Summary.Data.ListeningSockets[0].Owners[0].Command != testOriginalCommand {
		t.Fatal("output aliases retained history")
	}
}

func TestConnectionBudgetAccountsNestedCopies(t *testing.T) {
	s, _ := NewStore(config.Default())
	at := time.Unix(100, 0).UTC()
	command := strings.Repeat("x", 2048)
	pid := uint32(2)
	sockets := make([]metrics.ListeningSocket, 1, 1024)
	owners := make([]metrics.ProcessOwner, 1, 1024)
	owners[0] = metrics.ProcessOwner{Command: &command, PID: &pid}
	sockets[0] = metrics.ListeningSocket{
		Local:    metrics.Endpoint{Address: netip.MustParseAddr("::1"), HasPort: true},
		Protocol: 6,
		Owners:   owners,
	}
	value := metrics.Connections{ListeningSockets: sockets}
	_, charge := cloneConnections(value)
	if charge < len(command) {
		t.Fatal("nested string unaccounted")
	}
	s.connections.limit = charge * 2
	for i := 0; i < 3; i++ {
		if err := s.addConnections(
			collector.Sample[metrics.ConnectionsObservation]{
				FinishedAt: at.Add(time.Duration(i) * time.Second),
				Value:      metrics.ConnectionsObservation{Data: value},
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	if s.connections.bytes != charge*2 || len(s.connections.charges) != 2 {
		t.Fatalf(
			"bytes=%d entries=%d charge=%d",
			s.connections.bytes,
			len(s.connections.charges),
			charge,
		)
	}
	intervals, _ := s.connections.series.Read(
		aggregate.Window{Start: at, End: at.Add(3 * time.Second)},
	)
	for _, v := range intervals {
		if cap(v.Value.ListeningSockets) != 1 || cap(v.Value.ListeningSockets[0].Owners) != 1 {
			t.Fatal("retained oversized backing slices")
		}
	}
	got, _ := s.Snapshot(
		context.Background(),
		aggregate.Window{Start: at, End: at.Add(3 * time.Second)},
	)
	if math.Abs(got.Connections.Summary.CoverageRatio-2.0/3) > 1e-12 {
		t.Fatalf("eviction coverage %v", got.Connections.Summary.CoverageRatio)
	}
}
