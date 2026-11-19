package collector

import (
	"context"
	"fmt"
	"math"
	"path"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"unicode/utf8"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const darwinMountNoWait = 2

type darwinGetfsstat func([]syscall.Statfs_t, int) (int, error)

type DarwinFilesystemsSource struct {
	getfsstat darwinGetfsstat
}

var _ Source[metrics.FilesystemObservation] = (*DarwinFilesystemsSource)(nil)

func NewFilesystemsSource() *DarwinFilesystemsSource {
	return &DarwinFilesystemsSource{getfsstat: syscall.Getfsstat}
}

func readDarwinMounts(ctx context.Context, getfsstat darwinGetfsstat) ([]syscall.Statfs_t, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	count, err := getfsstat(nil, darwinMountNoWait)
	if err != nil {
		return nil, err
	}
	if count < 0 || count > maxMountRecords {
		return nil, fmt.Errorf("invalid mount count %d", count)
	}
	records := make([]syscall.Statfs_t, count+1)
	second, err := getfsstat(records, darwinMountNoWait)
	if err != nil {
		return nil, err
	}
	if second < 0 || second > maxMountRecords || second > count {
		return nil, fmt.Errorf("mount table grew during enumeration")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return records[:second], nil
}

func decodeDarwinText(data []int8) (string, error) {
	end := slices.Index(data, 0)
	if end < 0 {
		return "", fmt.Errorf("unterminated mount metadata")
	}
	if end == 0 {
		return "", fmt.Errorf("empty mount metadata")
	}
	raw := make([]byte, end)
	for i, value := range data[:end] {
		// #nosec G115 -- Conversion to uint8 preserves the original UTF-8 bytes.
		raw[i] = uint8(value)
	}
	text := string(raw)
	if !utf8.ValidString(text) {
		return "", fmt.Errorf("invalid mount metadata encoding")
	}
	return text, nil
}

func darwinUsage(raw syscall.Statfs_t) (metrics.FilesystemUsage, error) {
	kind, kindErr := decodeDarwinText(raw.Fstypename[:])
	mountPoint, mountErr := decodeDarwinText(raw.Mntonname[:])
	device, deviceErr := decodeDarwinText(raw.Mntfromname[:])
	identity := strings.Join([]string{
		device,
		strconv.FormatInt(int64(raw.Fsid.Val[0]), 10),
		strconv.FormatInt(int64(raw.Fsid.Val[1]), 10),
	}, ":")
	usage, err := filesystemUsage(filesystemMount{
		MountPoint: mountPoint, Device: device, FilesystemType: kind,
	}, filesystemStats{
		BlockSize:  uint64(raw.Bsize),
		Blocks:     raw.Blocks,
		FreeBlocks: raw.Bfree,
		Files:      raw.Files,
		FreeFiles:  raw.Ffree,
		InodesKnown: raw.Files > 0 && raw.Files != math.MaxUint64 &&
			raw.Ffree != math.MaxUint64,
	})
	if kindErr != nil {
		return metrics.FilesystemUsage{MountPoint: mountPoint, Identity: identity}, kindErr
	}
	if mountErr != nil {
		return metrics.FilesystemUsage{MountPoint: mountPoint, Identity: identity}, mountErr
	}
	if deviceErr != nil {
		return metrics.FilesystemUsage{MountPoint: mountPoint, Identity: identity}, deviceErr
	}
	if !path.IsAbs(mountPoint) {
		return metrics.FilesystemUsage{MountPoint: mountPoint, Identity: identity},
			fmt.Errorf("mount point must be absolute")
	}
	if err != nil {
		usage.MountPoint = mountPoint
		usage.Device = device
		usage.FilesystemType = kind
	}
	usage.Identity = identity
	return usage, err
}

type keyedDarwinRecord struct {
	raw        syscall.Statfs_t
	key        string
	mountPoint string
	err        error
}

func (s *DarwinFilesystemsSource) Collect(ctx context.Context) (metrics.FilesystemObservation, error) {
	records, err := readDarwinMounts(ctx, s.getfsstat)
	if err != nil {
		return metrics.FilesystemObservation{}, err
	}
	visible := make(map[string]keyedDarwinRecord)
	for i, raw := range records {
		kind, kindErr := decodeDarwinText(raw.Fstypename[:])
		if kindErr == nil && virtualFilesystem(kind) {
			continue
		}
		mountPoint, mountErr := decodeDarwinText(raw.Mntonname[:])
		key := mountPoint
		if mountErr != nil {
			key = fmt.Sprintf("\x00%d", i)
		} else if _, exists := visible[key]; exists {
			return metrics.FilesystemObservation{}, fmt.Errorf("duplicate mount point %q", key)
		}
		recordErr := kindErr
		if recordErr == nil {
			recordErr = mountErr
		}
		visible[key] = keyedDarwinRecord{raw: raw, key: key, mountPoint: mountPoint, err: recordErr}
	}
	if len(visible) > maxFilesystemMounts {
		return metrics.FilesystemObservation{}, fmt.Errorf("filesystem selection exceeds mount limit")
	}
	selected := make([]keyedDarwinRecord, 0, len(visible))
	for _, record := range visible {
		selected = append(selected, record)
	}
	slices.SortFunc(selected, func(a, b keyedDarwinRecord) int {
		return strings.Compare(a.key, b.key)
	})
	result := metrics.FilesystemObservation{Readings: make([]metrics.FilesystemReading, 0, len(selected))}
	for _, record := range selected {
		usage, err := darwinUsage(record.raw)
		if record.err != nil {
			err = record.err
			usage.MountPoint = record.mountPoint
		}
		result.Readings = append(result.Readings, metrics.FilesystemReading{
			MountPoint: record.mountPoint, Usage: usage, Err: err,
		})
	}
	return result, nil
}
