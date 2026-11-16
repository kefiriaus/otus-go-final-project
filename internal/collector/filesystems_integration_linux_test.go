package collector

import (
	"context"
	"errors"
	"io/fs"
	"math"
	"syscall"
	"testing"
	"testing/fstest"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const testOtherMount = "/other"

func filesystemIntegrationStatfs(iteration int) func(string, *syscall.Statfs_t) error {
	return func(name string, s *syscall.Statfs_t) error {
		if name == testDataMount && iteration == 1 {
			return fs.ErrPermission
		}
		free := uint64(128)
		if iteration == 0 {
			free = 192
		}
		if name == testDataMount {
			switch iteration {
			case 2:
				free = 64
			case 4:
				free = 0
			case 5:
				free = 128
			}
		}
		*s = syscall.Statfs_t{Bsize: 4096, Blocks: 256, Bfree: free, Files: 100, Ffree: 75}
		if name == testDataMount && iteration >= 4 {
			s.Files = 0
			s.Ffree = 0
		}
		return nil
	}
}

func recordFilesystemSample(
	t *testing.T,
	series *aggregate.GaugeSeries[metrics.FilesystemUsage],
	name string,
	observation metrics.FilesystemObservation,
	collectionErr error,
	at time.Time,
) {
	t.Helper()
	reading := metrics.FilesystemReading{MountPoint: name, Err: fs.ErrNotExist}
	for _, r := range observation.Readings {
		if r.MountPoint == name {
			reading = r
			break
		}
	}
	failure := reading.Err
	if collectionErr != nil {
		failure = collectionErr
	}
	if err := series.Add(aggregate.Event[metrics.FilesystemUsage]{
		At: at, Value: reading.Usage, Err: failure,
	}); err != nil {
		t.Fatal(err)
	}
}

type filesystemWindowCase struct {
	mount                   string
	start, end              int
	used, percent, coverage float64
	device                  string
}

func assertFilesystemWindow(
	t *testing.T,
	tc filesystemWindowCase,
	history *aggregate.GaugeSeries[metrics.FilesystemUsage],
	base time.Time,
) {
	t.Helper()
	window := aggregate.Window{
		Start: base.Add(time.Duration(tc.start) * time.Second),
		End:   base.Add(time.Duration(tc.end) * time.Second),
	}
	values, err := history.Read(window)
	if err != nil {
		t.Fatal(err)
	}
	got, err := aggregate.MeanFilesystemUsage(window, tc.mount, values)
	if err != nil || got.Data == nil || got.Data.Device != tc.device ||
		math.Abs(got.CoverageRatio-tc.coverage) > 1e-9 ||
		got.Partial != (tc.coverage < 1) {
		t.Fatalf("%+v %v", got, err)
	}
	if math.Abs(got.Data.UsedMebibytes-tc.used) > 1e-9 || got.Data.UsedPercent == nil ||
		math.Abs(*got.Data.UsedPercent-tc.percent) > 1e-9 {
		t.Fatalf("%+v", got.Data)
	}
	again, _ := history.Read(window)
	repeated, err := aggregate.MeanFilesystemUsage(window, tc.mount, again)
	if err != nil || repeated.Data == nil ||
		repeated.Data.UsedMebibytes != got.Data.UsedMebibytes ||
		repeated.CoverageRatio != got.CoverageRatio {
		t.Fatal("read mutated history")
	}
}

func TestFilesystemHistoryIntegration(t *testing.T) {
	histories := make(map[string]*aggregate.GaugeSeries[metrics.FilesystemUsage], 2)
	for _, name := range []string{testDataMount, testOtherMount} {
		series, err := aggregate.NewGaugeSeries[metrics.FilesystemUsage](10)
		if err != nil {
			t.Fatal(err)
		}
		histories[name] = series
	}
	source := NewFilesystemsSource()
	base := time.Unix(1000, 0).UTC()
	for i := 0; i <= 6; i++ {
		data := mountLine(
			1,
			0,
			testDataMount,
			"ext4",
			"diskA",
		) + mountLine(
			2,
			0,
			testOtherMount,
			"ext4",
			"diskB",
		)
		if i == 3 {
			data = mountLine(2, 0, testOtherMount, "ext4", "diskB")
		}
		if i >= 4 {
			data = mountLine(
				3,
				0,
				testDataMount,
				"xfs",
				"replacement",
			) + mountLine(
				2,
				0,
				testOtherMount,
				"ext4",
				"diskB",
			)
		}
		source.procFS = filesystemFixture(data)
		if i == 6 {
			source.procFS = fstest.MapFS{}
		}
		source.statfs = filesystemIntegrationStatfs(i)
		observation, err := source.Collect(context.Background())
		if i == 6 {
			if !errors.Is(err, fs.ErrNotExist) {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		for name, series := range histories {
			recordFilesystemSample(t, series, name, observation, err, base.Add(time.Duration(i)*time.Second))
		}
	}
	for _, tc := range []filesystemWindowCase{
		{testDataMount, 0, 3, .5, 50, 2.0 / 3, "diskA"},
		{testOtherMount, 0, 3, 1.25 / 3, 125.0 / 3, 1, "diskB"},
		{testDataMount, 2, 6, .75, 75, .5, "replacement"},
		{testOtherMount, 2, 6, .5, 50, 1, "diskB"},
	} {
		assertFilesystemWindow(t, tc, histories[tc.mount], base)
	}
}

func TestFilesystemsRealLinux(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := NewFilesystemsSource().Collect(ctx)
	if errors.Is(err, fs.ErrNotExist) {
		t.Skipf("mountinfo unavailable: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	previous := ""
	failed := 0
	for _, r := range got.Readings {
		if r.MountPoint == "" || (previous != "" && r.MountPoint <= previous) {
			t.Fatal("mounts not sorted and unique")
		}
		previous = r.MountPoint
		if r.Err != nil {
			failed++
			t.Logf("mount %q failed: %v", r.MountPoint, r.Err)
			continue
		}
		if math.IsNaN(r.Usage.UsedMebibytes) || math.IsInf(r.Usage.UsedMebibytes, 0) ||
			r.Usage.UsedMebibytes < 0 {
			t.Fatalf("%+v", r.Usage)
		}
		for _, p := range []*float64{r.Usage.UsedPercent, r.Usage.UsedInodesPercent} {
			if p != nil && (math.IsNaN(*p) || math.IsInf(*p, 0) || *p < 0 || *p > 100) {
				t.Fatalf("invalid percentage %+v", r.Usage)
			}
		}
	}
	t.Logf("real Linux filesystem readings: %d, failed: %d", len(got.Readings), failed)
}
