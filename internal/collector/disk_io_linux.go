package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const maxDiskDevices = 128

// DiskIOSource collects whole-device sysfs counters with bounded enumeration.
// Construct it with NewDiskIOSource. Inspect errors inside each observation.
type DiskIOSource struct{ sysFS fs.FS }

var _ Source[metrics.DiskIOObservation] = (*DiskIOSource)(nil)

// NewDiskIOSource reads /sys/block, which lists whole devices, not partitions.
func NewDiskIOSource() *DiskIOSource { return &DiskIOSource{sysFS: os.DirFS("/sys")} }

func (s *DiskIOSource) devices(ctx context.Context) ([]string, error) {
	file, err := s.sysFS.Open("block")
	if err != nil {
		return nil, fmt.Errorf("open /sys/block: %w", err)
	}
	dir, ok := file.(fs.ReadDirFile)
	if !ok {
		return nil, errors.Join(fmt.Errorf("/sys/block does not support directory enumeration"), file.Close())
	}
	var entries []fs.DirEntry
	var readErr error
	for len(entries) <= maxDiskDevices {
		if readErr = ctx.Err(); readErr != nil {
			break
		}
		var batch []fs.DirEntry
		batch, readErr = dir.ReadDir(maxDiskDevices + 1 - len(entries))
		entries = append(entries, batch...)
		if readErr == io.EOF {
			readErr = nil
			break
		}
		if readErr != nil {
			break
		}
		if len(batch) == 0 {
			readErr = io.ErrNoProgress
			break
		}
		if len(entries) > maxDiskDevices {
			break
		}
	}
	if err := errors.Join(readErr, file.Close(), ctx.Err()); err != nil {
		return nil, fmt.Errorf("enumerate /sys/block: %w", err)
	}
	if len(entries) > maxDiskDevices {
		return nil, fmt.Errorf("/sys/block exceeds %d-device limit", maxDiskDevices)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !fs.ValidPath(name) || name == "." || strings.ContainsAny(name, "/\x00") {
			return nil, fmt.Errorf("invalid block device name %q", name)
		}
		names = append(names, name)
	}
	slices.Sort(names)
	for i := 1; i < len(names); i++ {
		if names[i] == names[i-1] {
			return nil, fmt.Errorf("duplicate block device %q", names[i])
		}
	}
	return names, nil
}

func (s *DiskIOSource) readFile(ctx context.Context, name string, limit int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := s.sysFS.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open /sys/%s: %w", name, err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, int64(limit+1)))
	if err := errors.Join(readErr, file.Close(), ctx.Err()); err != nil {
		return nil, fmt.Errorf("read /sys/%s: %w", name, err)
	}
	if len(data) > limit {
		return nil, fmt.Errorf("/sys/%s exceeds %d-byte limit", name, limit)
	}
	return data, nil
}

func (s *DiskIOSource) readDevice(ctx context.Context, device string) (metrics.DiskIOCounters, error) {
	dev, err := s.readFile(ctx, "block/"+device+"/dev", 128)
	if err != nil {
		return metrics.DiskIOCounters{}, err
	}
	identity, err := parseDiskDeviceIdentity(dev)
	if err != nil {
		return metrics.DiskIOCounters{}, fmt.Errorf("device %s identity: %w", device, err)
	}
	stat, err := s.readFile(ctx, "block/"+device+"/stat", 4096)
	if err != nil {
		return metrics.DiskIOCounters{}, err
	}
	counters, err := parseDiskIOStat(stat)
	if err != nil {
		return metrics.DiskIOCounters{}, fmt.Errorf("device %s stat: %w", device, err)
	}
	// Recheck after stat so a changed identity cannot label replacement counters.
	currentDev, err := s.readFile(ctx, "block/"+device+"/dev", 128)
	if err != nil {
		return metrics.DiskIOCounters{}, err
	}
	currentIdentity, err := parseDiskDeviceIdentity(currentDev)
	if err != nil {
		return metrics.DiskIOCounters{}, fmt.Errorf("device %s identity recheck: %w", device, err)
	}
	if currentIdentity != identity {
		return metrics.DiskIOCounters{}, fmt.Errorf("device %s identity changed during collection", device)
	}
	counters.Identity = identity
	return counters, nil
}

// Collect isolates device failures in Readings. Enumeration or cancellation
// failures invalidate the entire observation. Synchronous sysfs operations are
// checked for cancellation but cannot be forcibly interrupted.
func (s *DiskIOSource) Collect(ctx context.Context) (metrics.DiskIOObservation, error) {
	if err := ctx.Err(); err != nil {
		return metrics.DiskIOObservation{}, err
	}
	names, err := s.devices(ctx)
	if err != nil {
		return metrics.DiskIOObservation{}, err
	}
	result := metrics.DiskIOObservation{Readings: make([]metrics.DiskIOReading, 0, len(names))}
	for _, device := range names {
		if err := ctx.Err(); err != nil {
			return metrics.DiskIOObservation{}, err
		}
		counters, err := s.readDevice(ctx, device)
		if err != nil {
			err = fmt.Errorf("disk %s: %w", device, err)
		}
		if cause := ctx.Err(); cause != nil {
			return metrics.DiskIOObservation{}, cause
		}
		result.Readings = append(result.Readings, metrics.DiskIOReading{Device: device, Counters: counters, Err: err})
	}
	if err := ctx.Err(); err != nil {
		return metrics.DiskIOObservation{}, err
	}
	return result, nil
}
