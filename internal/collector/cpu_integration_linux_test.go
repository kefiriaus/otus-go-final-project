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

func TestCPUHistoryIntegration(t *testing.T) {
	source := NewCPUSource()
	fixtures := fstest.MapFS{}
	source.procFS = fixtures
	series, err := aggregate.NewCounterSeries(6)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1000, 0).UTC()
	inputs := []string{
		"cpu 100 100 100 100 100 100 100 100 1 2\n",
		"cpu 120 110 115 140 105 103 102 105 1000 2000\n",
		"",
		"cpu 200 200 200 200 200 200 200 200 3000 4000\n",
		"cpu 220 210 215 240 205 203 202 205 5000 6000\n",
		"cpu 1 1 1 1 1 1 1 1 0 0\n",
		"cpu 21 11 16 41 6 4 3 6 7000 8000\n",
	}
	for i, input := range inputs {
		if input == "" {
			delete(fixtures, "stat")
		} else {
			fixtures["stat"] = &fstest.MapFile{Data: []byte(input)}
		}
		value, err := source.Collect(context.Background())
		if i == 2 && !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("expected read error: %v", err)
		}
		if i != 2 && err != nil {
			t.Fatal(err)
		}
		sample := Sample[metrics.CPUCounters]{
			StartedAt:  base.Add(time.Duration(i) * time.Second),
			FinishedAt: base.Add(time.Duration(i) * time.Second),
			Value:      value,
			Err:        err,
		}
		if err := series.Add(
			aggregate.Event[aggregate.CounterPoint]{
				At:    sample.FinishedAt,
				Value: aggregate.CPUCounterPoint(sample.Value),
				Err:   sample.Err,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		start, end int
		coverage   float64
	}{{0, 4, .5}, {3, 6, 2.0 / 3}} {
		window := aggregate.Window{
			Start: base.Add(time.Duration(tc.start) * time.Second),
			End:   base.Add(time.Duration(tc.end) * time.Second),
		}
		values, err := series.Read(window)
		if err != nil {
			t.Fatal(err)
		}
		got, err := aggregate.MeanCPUUsage(window, values)
		if err != nil || got.Data == nil || !got.Partial ||
			math.Abs(got.CoverageRatio-tc.coverage) > 1e-9 ||
			!got.LastSampleAt.Equal(window.End) {
			t.Fatalf("%+v %v", got, err)
		}
		percentages := []float64{
			got.Data.UserPercent,
			got.Data.SystemPercent,
			got.Data.IdlePercent,
			got.Data.IOWaitPercent,
			got.Data.StealPercent,
		}
		for i, want := range []float64{30, 20, 40, 5, 5} {
			if math.Abs(percentages[i]-want) > 1e-9 {
				t.Fatalf("category %d got %v want %v", i, percentages[i], want)
			}
		}
		again, _ := series.Read(window)
		repeated, err := aggregate.MeanCPUUsage(window, again)
		if err != nil || repeated.Data == nil || *repeated.Data != *got.Data ||
			repeated.CoverageRatio != got.CoverageRatio {
			t.Fatal("another read changed history")
		}
	}
}

func TestCPURealLinux(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := NewCPUSource().Collect(ctx)
	if errors.Is(err, fs.ErrNotExist) {
		t.Skipf("procfs CPU stat unavailable: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("real Linux aggregate CPU counters: %+v", got)
}
