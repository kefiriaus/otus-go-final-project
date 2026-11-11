package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const diskFixtureStat = "10 0 20 0 5 0 10 0 0 0 0\n"

func diskFixture(names ...string) fstest.MapFS {
	result := fstest.MapFS{"block": &fstest.MapFile{Mode: fs.ModeDir}}
	for _, name := range names {
		result["block/"+name+"/dev"] = &fstest.MapFile{Data: []byte("8:0\n")}
		result["block/"+name+"/stat"] = &fstest.MapFile{Data: []byte(diskFixtureStat)}
	}
	return result
}

type diskFS func(string) (fs.File, error)

func (f diskFS) Open(name string) (fs.File, error) { return f(name) }

type diskFile struct {
	io.Reader
	closed   bool
	closeErr error
}

func (f *diskFile) Stat() (fs.FileInfo, error) { return nil, errors.New("unexpected Stat") }
func (f *diskFile) Close() error               { f.closed = true; return f.closeErr }

type diskReader func([]byte) (int, error)

func (f diskReader) Read(p []byte) (int, error) { return f(p) }

type diskDirEntry string

func (e diskDirEntry) Name() string               { return string(e) }
func (e diskDirEntry) IsDir() bool                { return false }
func (e diskDirEntry) Type() fs.FileMode          { return fs.ModeSymlink }
func (e diskDirEntry) Info() (fs.FileInfo, error) { return nil, errors.New("unexpected Info") }

type diskDir struct {
	diskFile
	entries   []fs.DirEntry
	readErr   error
	requested int
}

func (d *diskDir) ReadDir(n int) ([]fs.DirEntry, error) {
	d.requested = n
	result := d.entries[:min(n, len(d.entries))]
	d.entries = d.entries[len(result):]
	if d.readErr != nil {
		return result, d.readErr
	}
	if len(d.entries) == 0 {
		return result, io.EOF
	}
	return result, nil
}

func TestDiskIOSource(t *testing.T) {
	names := []string{"sda", "nvme0n1", "mmcblk0", "dm-0", "md0", "loop0"}
	fixture := diskFixture(names...)
	var entries []fs.DirEntry
	for _, name := range names {
		entries = append(entries, diskDirEntry(name))
	}
	for _, partition := range []string{"sda/sda1", "nvme0n1/nvme0n1p1", "mmcblk0/mmcblk0p1"} {
		fixture["block/"+partition+"/stat"] = &fstest.MapFile{Data: []byte("invalid partition")}
	}
	source := NewDiskIOSource()
	source.sysFS = diskFS(func(name string) (fs.File, error) {
		if name == "block" {
			return &diskDir{entries: entries}, nil
		}
		return fixture.Open(name)
	})
	got, err := source.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var devices []string
	for _, r := range got.Readings {
		devices = append(devices, r.Device)
		if r.Err != nil || r.Counters.Identity != "8:0" || r.Counters.ReadsCompleted != 10 || r.Counters.SectorsWritten != 10 {
			t.Fatalf("%+v", r)
		}
	}
	if !reflect.DeepEqual(devices, []string{"dm-0", "loop0", "md0", "mmcblk0", "nvme0n1", "sda"}) {
		t.Fatal(devices)
	}
}
func TestDiskIOSourceDeviceErrors(t *testing.T) {
	sentinel := errors.New("device read failed")
	for _, mode := range []string{"missing", "dev parse", "stat parse", "open", "read", "close"} {
		t.Run(mode, func(t *testing.T) {
			fixture := diskFixture("bad", "good")
			target := "block/bad/stat"
			cause := sentinel
			switch mode {
			case "missing":
				delete(fixture, target)
				cause = fs.ErrNotExist
			case "dev parse":
				fixture["block/bad/dev"].Data = []byte("invalid")
				cause = nil
			case "stat parse":
				fixture[target].Data = []byte("invalid")
				cause = nil
			}
			file := &diskFile{Reader: strings.NewReader(diskFixtureStat)}
			if mode == "read" {
				file.Reader = diskReader(func(p []byte) (int, error) { return copy(p, diskFixtureStat), sentinel })
			}
			if mode == "close" {
				file.closeErr = sentinel
			}
			source := NewDiskIOSource()
			source.sysFS = diskFS(func(name string) (fs.File, error) {
				if name == target && (mode == "open" || mode == "read" || mode == "close") {
					if mode == "open" {
						return nil, sentinel
					}
					return file, nil
				}
				return fixture.Open(name)
			})
			got, err := source.Collect(context.Background())
			if err != nil || len(got.Readings) != 2 {
				t.Fatalf("%+v %v", got, err)
			}
			bad, good := got.Readings[0], got.Readings[1]
			if bad.Err == nil || bad.Counters != (metrics.DiskIOCounters{}) || good.Err != nil || good.Device != "good" {
				t.Fatalf("%+v", got)
			}
			if cause != nil && !errors.Is(bad.Err, cause) {
				t.Fatalf("lost cause: %v", bad.Err)
			}
			if (mode == "read" || mode == "close") && !file.closed {
				t.Fatal("file not closed")
			}
		})
	}
	fixture := diskFixture("bad")
	delete(fixture, "block/bad/stat")
	source := NewDiskIOSource()
	source.sysFS = fixture
	got, err := source.Collect(context.Background())
	if err != nil || len(got.Readings) != 1 || got.Readings[0].Err == nil {
		t.Fatalf("all failed: %+v %v", got, err)
	}
}
func TestDiskIOSourceEnumeration(t *testing.T) {
	source := NewDiskIOSource()
	source.sysFS = diskFixture()
	empty, err := source.Collect(context.Background())
	if err != nil || len(empty.Readings) != 0 {
		t.Fatalf("%+v %v", empty, err)
	}
	source.sysFS = fstest.MapFS{}
	if _, err := source.Collect(context.Background()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	sentinel := errors.New("directory failure")
	for _, mode := range []string{"read", "close", "over limit", "duplicate", "invalid name", "regular file"} {
		t.Run(mode, func(t *testing.T) {
			dir := &diskDir{entries: []fs.DirEntry{diskDirEntry("sda")}}
			cause := sentinel
			switch mode {
			case "read":
				dir.readErr = sentinel
			case "close":
				dir.closeErr = sentinel
			case "over limit":
				for i := 1; i < 129; i++ {
					dir.entries = append(dir.entries, diskDirEntry(fmt.Sprint(i)))
				}
				cause = nil
			case "duplicate":
				dir.entries = append(dir.entries, diskDirEntry("sda"))
				cause = nil
			case "invalid name":
				dir.entries = []fs.DirEntry{diskDirEntry("../escape")}
				cause = nil
			case "regular file":
				cause = nil
			}
			source := NewDiskIOSource()
			source.sysFS = diskFS(func(string) (fs.File, error) {
				if mode == "regular file" {
					return &dir.diskFile, nil
				}
				return dir, nil
			})
			got, err := source.Collect(context.Background())
			if err == nil || len(got.Readings) != 0 || !dir.closed {
				t.Fatalf("%+v %v closed=%v", got, err, dir.closed)
			}
			if cause != nil && !errors.Is(err, cause) {
				t.Fatal(err)
			}
			if mode != "regular file" && dir.requested != 129 {
				t.Fatalf("unbounded enumeration: %d", dir.requested)
			}
		})
	}
	fixture := diskFixture()
	for i := 0; i < 128; i++ {
		name := fmt.Sprintf("dev%03d", i)
		fixture["block/"+name+"/dev"] = &fstest.MapFile{Data: []byte("8:0")}
		fixture["block/"+name+"/stat"] = &fstest.MapFile{Data: []byte(diskFixtureStat)}
	}
	source.sysFS = fixture
	boundary, err := source.Collect(context.Background())
	if err != nil || len(boundary.Readings) != 128 {
		t.Fatalf("boundary: %d %v", len(boundary.Readings), err)
	}
}
func TestDiskIOSourceReadBounds(t *testing.T) {
	for _, target := range []string{"stat", "dev"} {
		limit, valid := 4096, strings.TrimSpace(diskFixtureStat)
		if target == "dev" {
			limit, valid = 128, "8:0"
		}
		for _, size := range []int{limit, limit + 1, limit + 100} {
			fixture := diskFixture("sda")
			var files []*diskFile
			source := NewDiskIOSource()
			source.sysFS = diskFS(func(name string) (fs.File, error) {
				if name != "block/sda/"+target {
					return fixture.Open(name)
				}
				reader := strings.NewReader(valid + strings.Repeat(" ", size-len(valid)))
				consumed := 0
				file := &diskFile{Reader: diskReader(func(p []byte) (int, error) {
					n, err := reader.Read(p)
					consumed += n
					if consumed > limit+1 {
						t.Errorf("read %d bytes, limit %d", consumed, limit+1)
					}
					return n, err
				})}
				files = append(files, file)
				return file, nil
			})
			got, err := source.Collect(context.Background())
			if err != nil || len(got.Readings) != 1 {
				t.Fatalf("%+v %v", got, err)
			}
			if (got.Readings[0].Err == nil) != (size == limit) {
				t.Fatalf("target=%s size=%d error=%v", target, size, got.Readings[0].Err)
			}
			for _, file := range files {
				if !file.closed {
					t.Fatal("file not closed")
				}
			}
		}
	}
}
func TestDiskIOSourceCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source := NewDiskIOSource()
	source.sysFS = diskFS(func(string) (fs.File, error) { t.Fatal("opened after cancellation"); return nil, nil })
	if _, err := source.Collect(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	fixture := diskFixture("sda", "sdb")
	file := &diskFile{Reader: diskReader(func(p []byte) (int, error) { cancel(); return copy(p, diskFixtureStat), io.EOF })}
	source.sysFS = diskFS(func(name string) (fs.File, error) {
		if name == "block/sda/stat" {
			return file, nil
		}
		if name == "block/sdb/dev" {
			t.Error("continued after cancellation")
		}
		return fixture.Open(name)
	})
	got, err := source.Collect(ctx)
	if !errors.Is(err, context.Canceled) || len(got.Readings) != 0 || !file.closed {
		t.Fatalf("%+v %v closed=%v", got, err, file.closed)
	}
}

func TestDiskIOSourceIdentityChangesDuringRead(t *testing.T) {
	fixture := diskFixture("sda")
	source := NewDiskIOSource()
	source.sysFS = diskFS(func(name string) (fs.File, error) {
		file, err := fixture.Open(name)
		if name == "block/sda/stat" {
			fixture["block/sda/dev"].Data = []byte("8:16")
		}
		return file, err
	})
	got, err := source.Collect(context.Background())
	if err != nil || len(got.Readings) != 1 || got.Readings[0].Err == nil || got.Readings[0].Counters != (metrics.DiskIOCounters{}) {
		t.Fatalf("accepted inconsistent device sample: %+v %v", got, err)
	}
}
