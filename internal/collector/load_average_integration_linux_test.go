package collector

import (
	"context"
	"errors"
	"io/fs"
	"math"
	"testing"
	"testing/fstest"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func TestLoadAverageHistoryIntegration(t *testing.T) {
	source := NewLoadAverageSource()
	fixtures := fstest.MapFS{}
	source.procFS = fixtures
	series, err := aggregate.NewGaugeSeries[metrics.LoadAverage](4)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1000, 0).UTC()
	for i, input := range []string{"10 20 30", "20 40 60", "", "40 80 120", "50 100 150"} {
		if input == "" {
			delete(fixtures, "loadavg")
		} else {
			fixtures["loadavg"] = &fstest.MapFile{Data: []byte(input)}
		}
		value, err := source.Collect(context.Background())
		sample := Sample[metrics.LoadAverage]{
			StartedAt:  base.Add(time.Duration(i) * time.Second),
			FinishedAt: base.Add(time.Duration(i) * time.Second),
			Value:      value,
			Err:        err,
		}
		if i == 2 && !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("expected missing read: %v", err)
		}
		if i != 2 && err != nil {
			t.Fatal(err)
		}
		if err := series.Add(
			aggregate.Event[metrics.LoadAverage]{
				At:    sample.FinishedAt,
				Value: sample.Value,
				Err:   sample.Err,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		start, end time.Duration
		want       float64
	}{{0, 4 * time.Second, 70.0 / 3}, {time.Second, 5 * time.Second, 110.0 / 3}} {
		window := aggregate.Window{Start: base.Add(tc.start), End: base.Add(tc.end)}
		values, err := series.Read(window)
		if err != nil {
			t.Fatal(err)
		}
		got, err := aggregate.MeanLoadAverage(window, values)
		if err != nil || got.Data == nil || !got.Partial || got.CoverageRatio != .75 {
			t.Fatalf("%+v %v", got, err)
		}
		for i, value := range []float64{got.Data.Load1, got.Data.Load5, got.Data.Load15} {
			if math.Abs(value-tc.want*float64(i+1)) > 1e-9 {
				t.Fatalf("component %d: got %v", i, value)
			}
		}
		again, _ := series.Read(window)
		repeated, err := aggregate.MeanLoadAverage(window, again)
		if err != nil || repeated.Data == nil || *repeated.Data != *got.Data ||
			!repeated.LastSampleAt.Equal(*got.LastSampleAt) {
			t.Fatal("read consumed history")
		}
	}
}

func TestLoadAverageRealLinux(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := NewLoadAverageSource().Collect(ctx)
	if errors.Is(err, fs.ErrNotExist) {
		t.Skipf("procfs load average unavailable: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []float64{got.Load1, got.Load5, got.Load15} {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			t.Fatalf("invalid indicator: %+v", got)
		}
	}
	t.Logf("real Linux load average: %+v", got)
}
