package collector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"syscall"
	"testing"
	"time"
)

func darwinTestRecord(kind, mount, device string) syscall.Statfs_t {
	var record syscall.Statfs_t
	for i, ch := range kind {
		// #nosec G115 -- Fixture metadata is ASCII.
		record.Fstypename[i] = int8(ch)
	}
	for i, ch := range mount {
		// #nosec G115 -- Fixture metadata is ASCII.
		record.Mntonname[i] = int8(ch)
	}
	for i, ch := range device {
		// #nosec G115 -- Fixture metadata is ASCII.
		record.Mntfromname[i] = int8(ch)
	}
	record.Bsize = 4096
	record.Blocks = 256
	record.Bfree = 128
	record.Files = 100
	record.Ffree = 75
	record.Fsid = syscall.Fsid{Val: [2]int32{1, 2}}
	return record
}

func darwinTestSource(records []syscall.Statfs_t) *DarwinFilesystemsSource {
	return &DarwinFilesystemsSource{getfsstat: func(buffer []syscall.Statfs_t, _ int) (int, error) {
		if len(buffer) == 0 {
			return len(records), nil
		}
		return copy(buffer, records), nil
	}}
}

func TestDarwinCollect(t *testing.T) {
	first := darwinTestRecord("apfs", "/z", "/dev/disk2s1")
	second := darwinTestRecord("apfs", "/a", "/dev/disk1s1")
	devfs := darwinTestRecord("devfs", "/dev", "devfs")
	fdesc := darwinTestRecord("fdesc", "/dev/fd", "fdesc")
	got, err := darwinTestSource([]syscall.Statfs_t{first, devfs, second, fdesc}).Collect(context.Background())
	if err != nil || len(got.Readings) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	a, z := got.Readings[0], got.Readings[1]
	if a.MountPoint != "/a" || z.MountPoint != "/z" || a.Err != nil || z.Err != nil {
		t.Fatalf("%+v", got.Readings)
	}
	if a.Usage.Identity != "/dev/disk1s1:1:2" || a.Usage.UsedMebibytes != 0.5 ||
		*a.Usage.UsedPercent != 50 || *a.Usage.UsedInodes != 25 ||
		*a.Usage.UsedInodesPercent != 25 {
		t.Fatalf("%+v", a.Usage)
	}
}

func TestDarwinUsageEdgeCases(t *testing.T) {
	zero := darwinTestRecord("apfs", "/zero", "/dev/zero")
	zero.Bfree = zero.Blocks
	zero.Ffree = zero.Files
	zeroTotal := darwinTestRecord("apfs", "/zero-total", "/dev/zero-total")
	zeroTotal.Blocks, zeroTotal.Bfree, zeroTotal.Files, zeroTotal.Ffree = 0, 0, 0, 0
	empty := darwinTestRecord("apfs", "/empty", "/dev/empty")
	empty.Bsize = 0
	invalid := darwinTestRecord("apfs", "/invalid", "/dev/invalid")
	invalid.Mntonname[0] = -1
	got, err := darwinTestSource([]syscall.Statfs_t{zero, zeroTotal, empty, invalid}).Collect(context.Background())
	if err != nil || len(got.Readings) != 4 {
		t.Fatalf("%+v %v", got, err)
	}
	invalidReading := got.Readings[0]
	emptyReading := got.Readings[1]
	zeroReading := got.Readings[2]
	zeroTotalReading := got.Readings[3]
	if zeroReading.Usage.UsedMebibytes != 0 || *zeroReading.Usage.UsedPercent != 0 ||
		*zeroReading.Usage.UsedInodes != 0 || *zeroReading.Usage.UsedInodesPercent != 0 {
		t.Fatalf("zero usage: %+v", zeroReading.Usage)
	}
	if zeroTotalReading.Usage.UsedPercent != nil || zeroTotalReading.Usage.UsedInodes != nil ||
		zeroTotalReading.Usage.UsedInodesPercent != nil {
		t.Fatalf("zero total: %+v", zeroTotalReading.Usage)
	}
	if emptyReading.Err == nil || emptyReading.MountPoint != "/empty" || invalidReading.Err == nil {
		t.Fatalf("invalid records: %+v", got.Readings)
	}
}

func TestDarwinMountLimit(t *testing.T) {
	records := make([]syscall.Statfs_t, maxMountRecords)
	got, err := readDarwinMounts(context.Background(), func(buffer []syscall.Statfs_t, _ int) (int, error) {
		if len(buffer) == 0 {
			return len(records), nil
		}
		return copy(buffer, records), nil
	})
	if err != nil || len(got) != maxMountRecords {
		t.Fatalf("mount limit: len=%d err=%v", len(got), err)
	}
}

func TestDarwinCollectErrors(t *testing.T) {
	duplicate := darwinTestRecord("apfs", "/a", "/dev/a")
	tooMany := make([]syscall.Statfs_t, 1025)
	for i := range tooMany {
		tooMany[i] = duplicate
	}
	for _, tt := range []struct {
		name    string
		records []syscall.Statfs_t
	}{
		{"duplicate", []syscall.Statfs_t{duplicate, duplicate}},
		{"too many", tooMany},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := darwinTestSource(tt.records).Collect(context.Background()); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	invalid := darwinTestRecord("apfs", "/invalid", "/dev/invalid")
	invalid.Mntonname[0] = -1
	got, err := darwinTestSource([]syscall.Statfs_t{invalid, duplicate}).Collect(context.Background())
	if err != nil || len(got.Readings) != 2 || got.Readings[1].Err == nil {
		t.Fatalf("%+v %v", got, err)
	}
	empty := darwinTestRecord("apfs", "", "/dev/empty")
	got, err = darwinTestSource([]syscall.Statfs_t{empty, duplicate}).Collect(context.Background())
	if err != nil || len(got.Readings) != 2 || got.Readings[1].Err == nil {
		t.Fatalf("%+v %v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := darwinTestSource(nil).Collect(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestDarwinUTF8(t *testing.T) {
	record := darwinTestRecord("apfs", "/数据", "/dev/disk")
	for i, value := range []byte("/数据") {
		// #nosec G115 -- UTF-8 bytes are intentionally stored as signed Darwin chars.
		record.Mntonname[i] = int8(value)
	}
	got, err := darwinTestSource([]syscall.Statfs_t{record}).Collect(context.Background())
	if err != nil || len(got.Readings) != 1 || got.Readings[0].Err != nil ||
		got.Readings[0].MountPoint != "/数据" {
		t.Fatalf("%+v %v", got, err)
	}
	record.Mntonname[0] = -1
	got, err = darwinTestSource([]syscall.Statfs_t{record}).Collect(context.Background())
	if err != nil || len(got.Readings) != 1 || got.Readings[0].Err == nil {
		t.Fatalf("invalid UTF-8: %+v %v", got, err)
	}
}

func TestDarwinSelectionLimit(t *testing.T) {
	invalid := darwinTestRecord("apfs", "/invalid", "/dev/invalid")
	invalid.Mntfromname[0] = -1
	records := make([]syscall.Statfs_t, 0, 129)
	for i := 0; i < 127; i++ {
		record := darwinTestRecord("apfs", "/invalid", "/dev/invalid")
		name := fmt.Sprintf("/invalid-%03d", i)
		for j, ch := range name {
			// #nosec G115 -- Fixture metadata is ASCII.
			record.Mntonname[j] = int8(ch)
		}
		record.Mntfromname[0] = -1
		records = append(records, record)
	}
	records = append(records, darwinTestRecord("apfs", "/valid", "/dev/valid"))
	got, err := darwinTestSource(records).Collect(context.Background())
	if err != nil || len(got.Readings) != 128 {
		t.Fatalf("128 readings: %+v %v", got, err)
	}
	records = append(records, invalid)
	if _, err := darwinTestSource(records).Collect(context.Background()); err == nil {
		t.Fatal("expected selection limit error")
	}
}

func TestDarwinUnterminatedMetadata(t *testing.T) {
	record := darwinTestRecord("apfs", "/a", "/dev/a")
	for i := range record.Mntonname {
		record.Mntonname[i] = 'x'
	}
	got, err := darwinTestSource([]syscall.Statfs_t{record}).Collect(context.Background())
	if err != nil || len(got.Readings) != 1 || got.Readings[0].Err == nil {
		t.Fatalf("unterminated metadata: %+v %v", got, err)
	}
}

func TestDarwinCollectSecondCallChange(t *testing.T) {
	record := darwinTestRecord("apfs", "/a", "/dev/a")
	calls := 0
	source := &DarwinFilesystemsSource{getfsstat: func(buffer []syscall.Statfs_t, _ int) (int, error) {
		calls++
		if calls == 1 {
			return 1, nil
		}
		return copy(buffer, []syscall.Statfs_t{record, record, record}), nil
	}}
	if _, err := source.Collect(context.Background()); err == nil {
		t.Fatal("expected buffer overflow error")
	}
}

func validDarwinOptional(value *float64) bool {
	return value == nil || !math.IsNaN(*value) && !math.IsInf(*value, 0) && *value >= 0 && *value <= 100
}

func TestFilesystemsRealDarwin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := NewFilesystemsSource().Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, reading := range got.Readings {
		if reading.Err != nil {
			continue
		}
		if !path.IsAbs(reading.MountPoint) || reading.Usage.Device == "" ||
			reading.Usage.FilesystemType == "" || reading.Usage.Identity == "" {
			t.Fatalf("invalid mount metadata: %+v", reading)
		}
		if math.IsNaN(reading.Usage.UsedMebibytes) || math.IsInf(reading.Usage.UsedMebibytes, 0) ||
			reading.Usage.UsedMebibytes < 0 {
			t.Fatalf("invalid used space: %+v", reading)
		}
		for _, value := range []*float64{reading.Usage.UsedPercent, reading.Usage.UsedInodesPercent} {
			if !validDarwinOptional(value) {
				t.Fatalf("invalid optional percentage: %+v", reading)
			}
		}
		if reading.Usage.UsedInodes != nil &&
			(math.IsNaN(*reading.Usage.UsedInodes) || math.IsInf(*reading.Usage.UsedInodes, 0) ||
				*reading.Usage.UsedInodes < 0) {
			t.Fatalf("invalid inode count: %+v", reading)
		}
		return
	}
	t.Fatal("no successful eligible filesystem reading")
}
