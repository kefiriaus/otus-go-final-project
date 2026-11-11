package aggregate

import (
	"fmt"
	"path"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func validFilesystemUsage(value metrics.FilesystemUsage, mount string) bool {
	if value.MountPoint != mount || value.Identity == "" || value.Device == "" || value.FilesystemType == "" || !finite(value.UsedMebibytes) || value.UsedMebibytes < 0 {
		return false
	}
	for _, number := range []*float64{value.UsedPercent, value.UsedInodes, value.UsedInodesPercent} {
		if number != nil && (!finite(*number) || *number < 0) {
			return false
		}
	}
	for _, percentage := range []*float64{value.UsedPercent, value.UsedInodesPercent} {
		if percentage != nil && *percentage > 100 {
			return false
		}
	}
	return value.UsedInodesPercent == nil || value.UsedInodes != nil
}

// MeanFilesystemUsage averages independent indicators for one mount point.
// Only the latest contributing mount identity is used, so a remount cannot mix
// old backing storage into the result. Optional fields use their own coverage.
func MeanFilesystemUsage(window Window, mountPoint string, values []Interval[metrics.FilesystemUsage]) (Summary[metrics.FilesystemUsage], error) {
	if !path.IsAbs(mountPoint) {
		return Summary[metrics.FilesystemUsage]{}, fmt.Errorf("filesystem mean requires an absolute mount point")
	}
	selection, err := selectIntervals(window, values)
	if err != nil {
		return Summary[metrics.FilesystemUsage]{}, err
	}
	latest := -1
	for i, v := range values {
		if !validFilesystemUsage(v.Value, mountPoint) {
			return Summary[metrics.FilesystemUsage]{}, fmt.Errorf("invalid filesystem usage or metadata")
		}
		if selection.durations[i] > 0 && (latest < 0 || v.ObservedAt.After(values[latest].ObservedAt)) {
			latest = i
		}
	}
	if latest < 0 {
		return Summary[metrics.FilesystemUsage]{}, nil
	}
	metadata := values[latest].Value
	var base []Interval[float64]
	var optional [3][]Interval[float64]
	for i, v := range values {
		if selection.durations[i] == 0 || v.Value.Identity != metadata.Identity {
			continue
		}
		if v.Value.Device != metadata.Device || v.Value.FilesystemType != metadata.FilesystemType {
			return Summary[metrics.FilesystemUsage]{}, fmt.Errorf("filesystem metadata changed within one identity")
		}
		scalar := Interval[float64]{Start: v.Start, End: v.End, ObservedAt: v.ObservedAt, Value: v.Value.UsedMebibytes, Partial: v.Partial}
		base = append(base, scalar)
		for field, number := range [3]*float64{v.Value.UsedPercent, v.Value.UsedInodes, v.Value.UsedInodesPercent} {
			if number != nil {
				scalar.Value = *number
				optional[field] = append(optional[field], scalar)
			}
		}
	}
	mean, err := WeightedMean(window, base)
	if err != nil {
		return Summary[metrics.FilesystemUsage]{}, err
	}
	data := metrics.FilesystemUsage{MountPoint: mountPoint, Device: metadata.Device, FilesystemType: metadata.FilesystemType, Identity: metadata.Identity, UsedMebibytes: *mean.Data}
	targets := [3]**float64{&data.UsedPercent, &data.UsedInodes, &data.UsedInodesPercent}
	partial := mean.Partial
	for field, intervals := range optional {
		result, err := WeightedMean(window, intervals)
		if err != nil {
			return Summary[metrics.FilesystemUsage]{}, err
		}
		*targets[field] = result.Data
		if result.Data != nil && result.CoverageRatio < mean.CoverageRatio {
			partial = true
		}
	}
	return Summary[metrics.FilesystemUsage]{Data: &data, CoverageRatio: mean.CoverageRatio, LastSampleAt: mean.LastSampleAt, Partial: partial}, nil
}
