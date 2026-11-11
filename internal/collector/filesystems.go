package collector

import (
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const maxMountInfoBytes = 1 << 20
const maxMountRecords = 1024
const maxFilesystemMounts = 128

type filesystemMount struct {
	ID             uint64
	ParentID       uint64
	MajorMinor     string
	Root           string
	MountPoint     string
	Device         string
	FilesystemType string
}
type filesystemStats struct {
	BlockSize   uint64
	Blocks      uint64
	FreeBlocks  uint64
	Files       uint64
	FreeFiles   uint64
	InodesKnown bool
}

func decodeMountField(value string) (string, error) {
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] != '\\' {
			if value[i] == 0 {
				return "", fmt.Errorf("NUL in mount field")
			}
			out.WriteByte(value[i])
			continue
		}
		if i+3 >= len(value) {
			return "", fmt.Errorf("truncated mount escape")
		}
		switch value[i+1 : i+4] {
		case "040":
			out.WriteByte(' ')
		case "011":
			out.WriteByte('\t')
		case "012":
			out.WriteByte('\n')
		case "134":
			out.WriteByte('\\')
		default:
			return "", fmt.Errorf("invalid mount escape %q", value[i:i+4])
		}
		i += 3
	}
	return out.String(), nil
}

func parseMountInfo(data []byte) ([]filesystemMount, error) {
	if len(data) > maxMountInfoBytes {
		return nil, fmt.Errorf("mountinfo exceeds byte limit")
	}
	lines := strings.Split(string(data), "\n")
	var result []filesystemMount
	ids := make(map[uint64]bool)
	for i, line := range lines {
		if line == "" && i == len(lines)-1 {
			continue
		}
		if len(line) > 1<<16 {
			return nil, fmt.Errorf("mountinfo line exceeds byte limit")
		}
		if len(result) >= maxMountRecords {
			return nil, fmt.Errorf("mountinfo exceeds record limit")
		}
		fields := strings.FieldsFunc(line, func(r rune) bool { return r == ' ' || r == '\t' })
		separator := -1
		for j := 6; j < len(fields); j++ {
			if fields[j] == "-" {
				separator = j
				break
			}
		}
		if separator < 6 || len(fields) != separator+4 {
			return nil, fmt.Errorf("mountinfo line %d: malformed fields", i+1)
		}
		id, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("mount ID: %w", err)
		}
		parent, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parent mount ID: %w", err)
		}
		if id == 0 || ids[id] {
			return nil, fmt.Errorf("zero or duplicate mount ID")
		}
		ids[id] = true
		majorMinor, err := parseDiskDeviceIdentity([]byte(fields[2]))
		if err != nil {
			return nil, fmt.Errorf("mount device: %w", err)
		}
		root, err := decodeMountField(fields[3])
		if err != nil {
			return nil, err
		}
		mount, err := decodeMountField(fields[4])
		if err != nil {
			return nil, err
		}
		device, err := decodeMountField(fields[separator+2])
		if err != nil {
			return nil, err
		}
		if !path.IsAbs(mount) {
			return nil, fmt.Errorf("mount point must be absolute")
		}
		result = append(result, filesystemMount{ID: id, ParentID: parent, MajorMinor: majorMinor, Root: root, MountPoint: mount, Device: device, FilesystemType: fields[separator+1]})
	}
	return result, nil
}

func virtualFilesystem(kind string) bool {
	switch kind {
	case "proc", "sysfs", "tmpfs", "devtmpfs", "devpts", "cgroup", "cgroup2", "securityfs", "pstore", "debugfs", "tracefs", "configfs", "fusectl", "hugetlbfs", "mqueue", "binfmt_misc", "rpc_pipefs", "nsfs", "autofs", "bpf", "selinuxfs", "efivarfs", "ramfs", "rootfs", "overlay", "aufs":
		return true
	}
	return false
}

func visibleFilesystemMounts(mounts []filesystemMount) ([]filesystemMount, error) {
	byID := make(map[uint64]filesystemMount, len(mounts))
	covered := make(map[uint64]bool)
	for _, m := range mounts {
		byID[m.ID] = m
	}
	for _, m := range mounts {
		current := m
		seen := make(map[uint64]bool)
		for {
			if seen[current.ID] {
				return nil, fmt.Errorf("mount parent cycle")
			}
			seen[current.ID] = true
			parent, ok := byID[current.ParentID]
			if !ok || parent.ID == current.ID {
				break
			}
			current = parent
		}
		if parent, ok := byID[m.ParentID]; ok && parent.ID != m.ID && parent.MountPoint == m.MountPoint {
			covered[parent.ID] = true
		}
	}
	visible := make(map[string]filesystemMount)
	for _, m := range mounts {
		if covered[m.ID] {
			continue
		}
		current, hidden := m, false
		for {
			parent, ok := byID[current.ParentID]
			if !ok || parent.ID == current.ID {
				break
			}
			if parent.MountPoint != current.MountPoint && covered[parent.ID] {
				hidden = true
				break
			}
			current = parent
		}
		if hidden {
			continue
		}
		if _, exists := visible[m.MountPoint]; exists {
			return nil, fmt.Errorf("ambiguous mount stack at %q", m.MountPoint)
		}
		visible[m.MountPoint] = m
	}
	var result []filesystemMount
	for _, m := range visible {
		if virtualFilesystem(m.FilesystemType) {
			continue
		}
		if len(result) >= maxFilesystemMounts {
			return nil, fmt.Errorf("filesystem selection exceeds mount limit")
		}
		result = append(result, m)
	}
	slices.SortFunc(result, func(a, b filesystemMount) int { return strings.Compare(a.MountPoint, b.MountPoint) })
	return result, nil
}

func filesystemUsage(m filesystemMount, s filesystemStats) (metrics.FilesystemUsage, error) {
	if s.BlockSize == 0 || s.FreeBlocks > s.Blocks || (s.InodesKnown && s.FreeFiles > s.Files) {
		return metrics.FilesystemUsage{}, fmt.Errorf("invalid filesystem total/free accounting")
	}
	used := s.Blocks - s.FreeBlocks
	result := metrics.FilesystemUsage{MountPoint: m.MountPoint, Device: m.Device, FilesystemType: m.FilesystemType, Identity: fmt.Sprintf("%d:%s:%s", m.ID, m.MajorMinor, m.Root), UsedMebibytes: float64(used) * (float64(s.BlockSize) / (1024 * 1024))}
	if s.Blocks > 0 {
		percentage := float64(used) / float64(s.Blocks) * 100
		result.UsedPercent = &percentage
	}
	if s.InodesKnown && s.Files > 0 {
		count := float64(s.Files - s.FreeFiles)
		percentage := count / float64(s.Files) * 100
		result.UsedInodes = &count
		result.UsedInodesPercent = &percentage
	}
	return result, nil
}
