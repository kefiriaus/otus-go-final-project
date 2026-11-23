package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"syscall"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

// FilesystemsSource reads visible storage mounts in this process's namespace.
// Construct it with NewFilesystemsSource; inspect errors inside each reading.
type FilesystemsSource struct {
	procFS fs.FS
	statfs func(string, *syscall.Statfs_t) error
}

var _ Source[metrics.FilesystemObservation] = (*FilesystemsSource)(nil)

// NewFilesystemsSource combines /proc/self/mountinfo with Linux statfs.
func NewFilesystemsSource() *FilesystemsSource {
	return &FilesystemsSource{procFS: os.DirFS("/proc"), statfs: syscall.Statfs}
}

// Collect preserves per-mount failures. Enumeration and cancellation invalidate
// the entire observation. Synchronous statfs may block on a filesystem; context
// is checked around calls, but cannot force a blocked syscall to return.
func (s *FilesystemsSource) Collect(ctx context.Context) (metrics.FilesystemObservation, error) {
	if err := ctx.Err(); err != nil {
		return metrics.FilesystemObservation{}, err
	}
	file, err := s.procFS.Open("self/mountinfo")
	if err != nil {
		return metrics.FilesystemObservation{}, fmt.Errorf("open mountinfo: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxMountInfoBytes+1))
	if err := errors.Join(readErr, file.Close(), ctx.Err()); err != nil {
		return metrics.FilesystemObservation{}, fmt.Errorf("read mountinfo: %w", err)
	}
	mounts, err := parseMountInfo(data)
	if err != nil {
		return metrics.FilesystemObservation{}, err
	}
	selected, err := visibleFilesystemMounts(mounts)
	if err != nil {
		return metrics.FilesystemObservation{}, err
	}
	result := metrics.FilesystemObservation{
		Readings: make([]metrics.FilesystemReading, 0, len(selected)),
	}
	for _, mount := range selected {
		if err := ctx.Err(); err != nil {
			return metrics.FilesystemObservation{}, err
		}
		var raw syscall.Statfs_t
		err := s.statfs(mount.MountPoint, &raw)
		if cause := ctx.Err(); cause != nil {
			return metrics.FilesystemObservation{}, cause
		}
		var usage metrics.FilesystemUsage
		if err == nil {
			size := raw.Frsize
			if size <= 0 {
				size = raw.Bsize
			}
			if size <= 0 {
				err = fmt.Errorf("nonpositive filesystem accounting block size")
			} else {
				usage, err = filesystemUsage(
					mount,
					filesystemStats{
						BlockSize:  uint64(size),
						Blocks:     raw.Blocks,
						FreeBlocks: raw.Bfree,
						Files:      raw.Files,
						FreeFiles:  raw.Ffree,
						InodesKnown: raw.Files > 0 && raw.Files != math.MaxUint64 &&
							raw.Ffree != math.MaxUint64,
					},
				)
			}
		}
		if err != nil {
			err = fmt.Errorf("filesystem %q: %w", mount.MountPoint, err)
		}
		result.Readings = append(
			result.Readings,
			metrics.FilesystemReading{MountPoint: mount.MountPoint, Usage: usage, Err: err},
		)
	}
	if err := ctx.Err(); err != nil {
		return metrics.FilesystemObservation{}, err
	}
	return result, nil
}
