package main

import (
	"fmt"
	"strings"

	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
)

func formatLoadAverage(section *statsv1.LoadAverageSection) (string, error) {
	if section == nil {
		return "", fmt.Errorf("nil load average section")
	}
	status, err := formatStatus("load_average", section.Status)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	out.WriteString(status)
	if section.Data != nil {
		fmt.Fprintf(&out, "  load1=%.2f load5=%.2f load15=%.2f\n",
			section.Data.Load1, section.Data.Load5, section.Data.Load15)
	}
	return out.String(), nil
}

func formatDiskIO(section *statsv1.DiskIoSection) (string, error) {
	if section == nil {
		return "", fmt.Errorf("nil disk I/O section")
	}
	status, err := formatStatus("disk_io", section.Status)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	out.WriteString(status)
	if section.Data == nil {
		return out.String(), nil
	}
	if len(section.Data.Devices) == 0 {
		out.WriteString("  devices: empty\n")
	}
	for i, device := range section.Data.Devices {
		if device == nil {
			return "", fmt.Errorf("nil disk device at index %d", i)
		}
		fmt.Fprintf(&out, "  device=%s transfers_s=%.2f kib_s=%.2f\n",
			device.Device, device.TransfersPerSecond, device.KibibytesPerSecond)
	}
	return out.String(), nil
}

func formatOptionalNumber(value *float64, suffix string) string {
	if value == nil {
		return notAvailable
	}
	return fmt.Sprintf("%.2f%s", *value, suffix)
}

func formatFilesystems(section *statsv1.FilesystemsSection) (string, error) {
	if section == nil {
		return "", fmt.Errorf("nil filesystems section")
	}
	status, err := formatStatus("filesystems", section.Status)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	out.WriteString(status)
	if section.Data == nil {
		return out.String(), nil
	}
	if len(section.Data.Filesystems) == 0 {
		out.WriteString("  filesystems: empty\n")
	}
	for i, filesystem := range section.Data.Filesystems {
		if filesystem == nil {
			return "", fmt.Errorf("nil filesystem at index %d", i)
		}
		fmt.Fprintf(&out, "  mount=%s device=%s type=%s used_mib=%.2f used_percent=%s used_inodes=%s"+
			" used_inodes_percent=%s\n",
			filesystem.MountPoint, filesystem.Device, filesystem.FilesystemType,
			filesystem.UsedMebibytes,
			formatOptionalNumber(filesystem.UsedPercent, "%"),
			formatOptionalNumber(filesystem.UsedInodes, ""),
			formatOptionalNumber(filesystem.UsedInodesPercent, "%"))
	}
	return out.String(), nil
}
