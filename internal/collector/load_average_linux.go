package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

// LoadAverageSource reads Linux system load indicators from procfs.
// Construct it with NewLoadAverageSource.
type LoadAverageSource struct{ procFS fs.FS }

var _ Source[metrics.LoadAverage] = (*LoadAverageSource)(nil)

// NewLoadAverageSource returns a source reading /proc/loadavg directly.
func NewLoadAverageSource() *LoadAverageSource { return &LoadAverageSource{procFS: os.DirFS("/proc")} }

// Collect reads a bounded procfs file, preserving read and cancellation errors.
// Synchronous procfs reads cannot be forcibly canceled; cancellation is checked
// before opening and after reading, without abandoning a goroutine.
func (s *LoadAverageSource) Collect(ctx context.Context) (metrics.LoadAverage, error) {
	if err := ctx.Err(); err != nil {
		return metrics.LoadAverage{}, err
	}
	file, err := s.procFS.Open("loadavg")
	if err != nil {
		return metrics.LoadAverage{}, fmt.Errorf("open /proc/loadavg: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, 4097))
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr, ctx.Err()); err != nil {
		return metrics.LoadAverage{}, fmt.Errorf("read /proc/loadavg: %w", err)
	}
	if len(data) > 4096 {
		return metrics.LoadAverage{}, fmt.Errorf("/proc/loadavg exceeds 4096-byte limit")
	}
	return parseLoadAverage(data)
}
