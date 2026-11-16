package collector

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"testing"
	"testing/fstest"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func diskIntegrationStat(step, factor int) []byte {
	return []byte(
		fmt.Sprintf(
			"%d 99 %d 88 %d 77 %d 66 0 0 0 1000 2000 3000 4000 5000 6000\n",
			step*10*factor,
			step*20*factor,
			step*5*factor,
			step*10*factor,
		),
	)
}

func recordDiskDeviceSample(
	t *testing.T,
	history *aggregate.CounterSeries,
	name string,
	sample Sample[metrics.DiskIOObservation],
	iteration int,
) {
	t.Helper()
	reading := metrics.DiskIOReading{Device: name, Err: fs.ErrNotExist}
	for _, candidate := range sample.Value.Readings {
		if candidate.Device == name {
			reading = candidate
			break
		}
	}
	failure := reading.Err
	if sample.Err != nil {
		failure = sample.Err
	}
	if name == testSCSIDevice && (iteration == 2 || iteration == 4) && failure == nil {
		t.Fatal("missing per-device interruption")
	}
	if name == testNVMeDevice && iteration != 11 && failure != nil {
		t.Fatalf("other device interrupted: %v", failure)
	}
	if err := history.Add(aggregate.Event[aggregate.CounterPoint]{
		At: sample.FinishedAt, Value: aggregate.DiskIOCounterPoint(reading.Counters), Err: failure,
	}); err != nil {
		t.Fatal(err)
	}
}

type diskWindowCase struct {
	start, end                int
	sdaCoverage, nvmeCoverage float64
	lastSDA                   int
}

func assertDiskDeviceWindow(
	t *testing.T,
	tc diskWindowCase,
	name string,
	history *aggregate.CounterSeries,
	window aggregate.Window,
	base time.Time,
) {
	t.Helper()
	values, err := history.Read(window)
	if err != nil {
		t.Fatal(err)
	}
	got, err := aggregate.MeanDiskIO(window, name, values)
	wantCoverage, wantRate, last := tc.sdaCoverage, 15.0, base.Add(
		time.Duration(tc.lastSDA)*time.Second,
	)
	if name == testNVMeDevice {
		wantCoverage = tc.nvmeCoverage
		wantRate = 30
		last = window.End
	}
	if err != nil || got.Data == nil || got.Data.Device != name ||
		math.Abs(got.CoverageRatio-wantCoverage) > 1e-9 ||
		got.Partial != (wantCoverage < 1) || !got.LastSampleAt.Equal(last) {
		t.Fatalf("%s [%d,%d): %+v %v", name, tc.start, tc.end, got, err)
	}
	if math.Abs(got.Data.TransfersPerSecond-wantRate) > 1e-9 ||
		math.Abs(got.Data.KibibytesPerSecond-wantRate) > 1e-9 {
		t.Fatalf("%+v", got.Data)
	}
	again, _ := history.Read(window)
	repeated, err := aggregate.MeanDiskIO(window, name, again)
	if err != nil || repeated.Data == nil || *repeated.Data != *got.Data ||
		repeated.CoverageRatio != got.CoverageRatio {
		t.Fatal("read consumed history")
	}
}

func TestDiskIOHistoryIntegration(t *testing.T) {
	source := NewDiskIOSource()
	// A fixed two-device registry is test wiring, not an application registry.
	histories := make(map[string]*aggregate.CounterSeries, 2)
	for _, name := range []string{testSCSIDevice, testNVMeDevice} {
		s, err := aggregate.NewCounterSeries(20)
		if err != nil {
			t.Fatal(err)
		}
		histories[name] = s
	}
	base := time.Unix(1000, 0).UTC()
	for i := 0; i <= 13; i++ {
		fixture := diskFixture(testSCSIDevice, testNVMeDevice)
		step := i
		if i >= 9 {
			step = i - 9
		}
		fixture["block/sda/stat"].Data = diskIntegrationStat(step, 1)
		fixture["block/nvme0n1/stat"].Data = diskIntegrationStat(i, 2)
		fixture["block/nvme0n1/dev"].Data = []byte("259:0")
		if i >= 7 {
			fixture["block/sda/dev"].Data = []byte("8:16")
		}
		switch i {
		case 2:
			fixture["block/sda/stat"].Data = []byte("malformed stat")
		case 4:
			delete(fixture, "block/sda/stat")
			delete(fixture, "block/sda/dev")
		case 11:
			fixture = fstest.MapFS{}
		}
		source.sysFS = fixture
		observation, err := source.Collect(context.Background())
		if i == 11 {
			if !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("expected enumeration failure: %v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		sample := Sample[metrics.DiskIOObservation]{
			StartedAt:  base.Add(time.Duration(i) * time.Second),
			FinishedAt: base.Add(time.Duration(i) * time.Second),
			Value:      observation,
			Err:        err,
		}
		for name, history := range histories {
			recordDiskDeviceSample(t, history, name, sample, i)
		}
	}
	for _, tc := range []diskWindowCase{
		{0, 4, .25, 1, 1}, {4, 10, .5, 1, 10}, {10, 13, 1.0 / 3, 1.0 / 3, 13},
	} {
		window := aggregate.Window{
			Start: base.Add(time.Duration(tc.start) * time.Second),
			End:   base.Add(time.Duration(tc.end) * time.Second),
		}
		for name, history := range histories {
			assertDiskDeviceWindow(t, tc, name, history, window, base)
		}
	}
}

func TestDiskIORealLinux(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := NewDiskIOSource().Collect(ctx)
	if errors.Is(err, fs.ErrNotExist) {
		t.Skipf("sysfs block enumeration unavailable: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	failed := 0
	previous := ""
	for _, reading := range got.Readings {
		if reading.Device == "" || (previous != "" && reading.Device <= previous) {
			t.Fatal("devices not sorted and unique")
		}
		previous = reading.Device
		if reading.Err != nil {
			failed++
			t.Logf("device %s unavailable: %v", reading.Device, reading.Err)
			if reading.Counters != (metrics.DiskIOCounters{}) {
				t.Fatal("failed reading contains usable counters")
			}
			continue
		}
		identity, err := parseDiskDeviceIdentity([]byte(reading.Counters.Identity))
		if err != nil || identity != reading.Counters.Identity {
			t.Fatalf("%+v identity error %v", reading, err)
		}
	}
	t.Logf("real Linux whole-device readings: %d, failed: %d", len(got.Readings), failed)
}
