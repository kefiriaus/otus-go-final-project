package collector

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"math"
	"strings"
	"syscall"
	"testing"
	"testing/fstest"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const testDataMount = "/data"

func filesystemFixture(data string) fstest.MapFS {
	return fstest.MapFS{"self/mountinfo": &fstest.MapFile{Data: []byte(data)}}
}

func TestFilesystemsSource(t *testing.T) {
	source := NewFilesystemsSource()
	source.procFS = filesystemFixture(
		mountLine(
			1,
			0,
			"/",
			"ext4",
			"disk",
		) + mountLine(
			2,
			1,
			testDataMount,
			"xfs",
			"other",
		) + mountLine(
			3,
			1,
			"/proc",
			"proc",
			"proc",
		),
	)
	source.statfs = func(path string, s *syscall.Statfs_t) error {
		if path == "/proc" {
			t.Fatal("queried excluded filesystem")
		}
		*s = syscall.Statfs_t{
			Bsize:  8192,
			Frsize: 4096,
			Blocks: 1024,
			Bfree:  256,
			Bavail: 1,
			Files:  100,
			Ffree:  25,
		}
		return nil
	}
	got, err := source.Collect(context.Background())
	if err != nil || len(got.Readings) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	for _, r := range got.Readings {
		if r.Err != nil || r.Usage.UsedMebibytes != 3 || r.Usage.UsedPercent == nil ||
			*r.Usage.UsedPercent != 75 ||
			r.Usage.UsedInodes == nil ||
			*r.Usage.UsedInodes != 75 {
			t.Fatalf("%+v", r)
		}
	}
	source.statfs = func(_ string, s *syscall.Statfs_t) error {
		*s = syscall.Statfs_t{Bsize: 4096, Blocks: 256}
		return nil
	}
	got, err = source.Collect(context.Background())
	if err != nil || got.Readings[0].Usage.UsedMebibytes != 1 {
		t.Fatalf("Frsize fallback: %+v %v", got, err)
	}
	for _, stats := range []syscall.Statfs_t{
		{Bsize: 4096},
		{Bsize: 4096, Files: math.MaxUint64},
		{Bsize: 4096, Files: 100, Ffree: math.MaxUint64},
	} {
		source.statfs = func(_ string, s *syscall.Statfs_t) error { *s = stats; return nil }
		got, err = source.Collect(context.Background())
		if err != nil || got.Readings[0].Err != nil || got.Readings[0].Usage.UsedInodes != nil {
			t.Fatalf("unknown inodes: %+v %v", got, err)
		}
	}
}

func TestFilesystemsSourceFailures(t *testing.T) {
	sentinel := errors.New("statfs failure")
	source := NewFilesystemsSource()
	source.procFS = filesystemFixture(
		mountLine(1, 0, "/", "ext4", "disk") + mountLine(2, 1, testDataMount, "xfs", "other"),
	)
	source.statfs = func(path string, s *syscall.Statfs_t) error {
		if path == testDataMount {
			return sentinel
		}
		*s = syscall.Statfs_t{Bsize: 4096}
		return nil
	}
	got, err := source.Collect(context.Background())
	if err != nil || len(got.Readings) != 2 || got.Readings[0].Err != nil ||
		!errors.Is(got.Readings[1].Err, sentinel) ||
		got.Readings[1].Usage != (metrics.FilesystemUsage{}) {
		t.Fatalf("%+v %v", got, err)
	}
	source.statfs = func(string, *syscall.Statfs_t) error { return sentinel }
	got, err = source.Collect(context.Background())
	if err != nil || len(got.Readings) != 2 || got.Readings[0].Err == nil ||
		got.Readings[1].Err == nil {
		t.Fatalf("all failed: %+v %v", got, err)
	}
	for _, stats := range []syscall.Statfs_t{
		{Bsize: 0},
		{Bsize: 4096, Blocks: 1, Bfree: 2},
		{Bsize: 4096, Files: 1, Ffree: 2},
	} {
		source.statfs = func(_ string, s *syscall.Statfs_t) error { *s = stats; return nil }
		got, err = source.Collect(context.Background())
		if err != nil || got.Readings[0].Err == nil {
			t.Fatalf("invalid stats: %+v %v", got, err)
		}
	}
}

func TestFilesystemsSourceMountInfoErrors(t *testing.T) {
	sentinel := errors.New("mountinfo failure")
	for _, mode := range []string{testMissingMode, testOpenMode, "read", testCloseMode, testParseMode, testOversizedMode} {
		t.Run(mode, func(t *testing.T) {
			source := NewFilesystemsSource()
			file := &diskFile{Reader: strings.NewReader(mountLine(1, 0, "/", "ext4", "disk"))}
			cause := sentinel
			switch mode {
			case testMissingMode:
				source.procFS = fstest.MapFS{}
				cause = fs.ErrNotExist
			case testOpenMode:
				source.procFS = diskFS(func(string) (fs.File, error) { return nil, sentinel })
			case "read":
				file.Reader = diskReader(
					func(p []byte) (int, error) { return copy(p, mountLine(1, 0, "/", "ext4", "disk")), sentinel },
				)
			case testCloseMode:
				file.closeErr = sentinel
			case testParseMode:
				file.Reader = strings.NewReader("bad row")
				cause = nil
			case testOversizedMode:
				file.Reader = strings.NewReader(strings.Repeat("x", maxMountInfoBytes+100))
				cause = nil
			}
			consumed := 0
			original := file.Reader
			file.Reader = diskReader(
				func(p []byte) (int, error) { n, err := original.Read(p); consumed += n; return n, err },
			)
			if mode != testMissingMode && mode != testOpenMode {
				source.procFS = diskFS(func(name string) (fs.File, error) {
					if name != "self/mountinfo" {
						t.Fatal(name)
					}
					return file, nil
				})
			}
			source.statfs = func(string, *syscall.Statfs_t) error { t.Fatal("queried after enumeration failure"); return nil }
			got, err := source.Collect(context.Background())
			if err == nil || len(got.Readings) != 0 || (cause != nil && !errors.Is(err, cause)) {
				t.Fatalf("%+v %v", got, err)
			}
			if mode != testMissingMode && mode != testOpenMode && !file.closed {
				t.Fatal("file not closed")
			}
			if consumed > maxMountInfoBytes+1 {
				t.Fatal("unbounded read")
			}
		})
	}
	source := NewFilesystemsSource()
	source.procFS = filesystemFixture(mountLine(1, 0, "/proc", "proc", "proc"))
	source.statfs = func(string, *syscall.Statfs_t) error { t.Fatal("excluded mount queried"); return nil }
	got, err := source.Collect(context.Background())
	if err != nil || len(got.Readings) != 0 {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestFilesystemsSourceCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source := NewFilesystemsSource()
	source.procFS = diskFS(
		func(string) (fs.File, error) { t.Fatal("opened after cancellation"); return nil, nil },
	)
	if _, err := source.Collect(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	source.procFS = filesystemFixture(
		mountLine(1, 0, "/", "ext4", "disk") + mountLine(2, 1, testDataMount, "ext4", "other"),
	)
	calls := 0
	source.statfs = func(_ string, s *syscall.Statfs_t) error {
		calls++
		cancel()
		*s = syscall.Statfs_t{Bsize: 4096}
		return nil
	}
	got, err := source.Collect(ctx)
	if !errors.Is(err, context.Canceled) || len(got.Readings) != 0 || calls != 1 {
		t.Fatalf("%+v %v calls=%d", got, err, calls)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	file := &diskFile{
		Reader: diskReader(
			func(p []byte) (int, error) { cancel(); return copy(p, mountLine(1, 0, "/", "ext4", "disk")), io.EOF },
		),
	}
	source.procFS = diskFS(func(string) (fs.File, error) { return file, nil })
	if _, err := source.Collect(ctx); !errors.Is(err, context.Canceled) || !file.closed {
		t.Fatalf("%v closed=%v", err, file.closed)
	}
}
