package collector

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

// CPUSource reads the aggregate CPU accounting line from Linux procfs.
// Construct it with NewCPUSource.
type CPUSource struct{ procFS fs.FS }

var _ Source[metrics.CPUCounters] = (*CPUSource)(nil)

// cpuStatReader preserves an underlying failure even when a buffered reader
// finds a complete line before examining its pending error.
type cpuStatReader struct {
	io.Reader
	err error
}

func (r *cpuStatReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err != nil && err != io.EOF {
		r.err = err
	}
	return n, err
}

// NewCPUSource returns a source reading /proc/stat directly.
func NewCPUSource() *CPUSource { return &CPUSource{procFS: os.DirFS("/proc")} }

// Collect reads only the first line, with at most 4097 bytes prefetched.
// Synchronous procfs reads cannot be forcibly canceled; cancellation is checked
// before opening and after reading, without abandoning a goroutine.
func (s *CPUSource) Collect(ctx context.Context) (metrics.CPUCounters, error) {
	if err := ctx.Err(); err != nil {
		return metrics.CPUCounters{}, err
	}
	file, err := s.procFS.Open("stat")
	if err != nil {
		return metrics.CPUCounters{}, fmt.Errorf("open /proc/stat: %w", err)
	}
	reader := cpuStatReader{Reader: file}
	line, readErr := bufio.NewReaderSize(&reader, 4097).ReadSlice('\n')
	if reader.err != nil {
		readErr = reader.err
	}
	if readErr == io.EOF {
		readErr = nil
	}
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr, ctx.Err()); err != nil {
		return metrics.CPUCounters{}, fmt.Errorf("read /proc/stat CPU line: %w", err)
	}
	if len(line) > 4096 {
		return metrics.CPUCounters{}, fmt.Errorf("/proc/stat CPU line exceeds 4096-byte limit")
	}
	return parseCPUStatLine(line)
}
