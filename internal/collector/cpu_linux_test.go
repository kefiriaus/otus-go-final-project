package collector

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const testStatName = "stat"

type cpuFS func(string) (fs.File, error)

func (f cpuFS) Open(name string) (fs.File, error) { return f(name) }

type cpuFile struct {
	io.Reader
	closed   bool
	closeErr error
}

func (f *cpuFile) Stat() (fs.FileInfo, error) { return nil, errors.New("unexpected Stat") }
func (f *cpuFile) Close() error               { f.closed = true; return f.closeErr }

type cpuReader func([]byte) (int, error)

func (f cpuReader) Read(p []byte) (int, error) { return f(p) }

func TestCPUSource(t *testing.T) {
	for _, input := range []string{
		"cpu 20 10 15 40 5 3 2 5 100 200\ncpu0 999 999 999 999\nintr invalid\n",
		"cpu 20 10 15 40 5 3 2 5 100 200",
	} {
		source := NewCPUSource()
		source.procFS = fstest.MapFS{testStatName: &fstest.MapFile{Data: []byte(input)}}
		got, err := source.Collect(context.Background())
		want := metrics.CPUCounters{
			User:    20,
			Nice:    10,
			System:  15,
			Idle:    40,
			IOWait:  5,
			IRQ:     3,
			SoftIRQ: 2,
			Steal:   5,
		}
		if err != nil || got != want {
			t.Fatalf("got %+v %v, want %+v", got, err, want)
		}
	}
}

func TestCPUSourceErrors(t *testing.T) {
	sentinel := errors.New("CPU read failed")
	for _, mode := range []string{
		testMissingMode, testOpenMode, testReadMode, testCloseMode,
		"empty", "label", testParseMode, testOversizedMode,
	} {
		t.Run(mode, func(t *testing.T) {
			source := NewCPUSource()
			file := &cpuFile{Reader: strings.NewReader("cpu 1 2 3 4\n")}
			cause := sentinel
			switch mode {
			case testMissingMode:
				source.procFS = fstest.MapFS{}
				cause = fs.ErrNotExist
			case testOpenMode:
				source.procFS = cpuFS(func(string) (fs.File, error) { return nil, sentinel })
			case testReadMode:
				file.Reader = cpuReader(
					func(p []byte) (int, error) { return copy(p, "cpu 1 2 3 4"), sentinel },
				)
			case testCloseMode:
				file.closeErr = sentinel
			case "empty":
				file.Reader = strings.NewReader("")
				cause = nil
			case "label":
				file.Reader = strings.NewReader("cpu0 1 2 3 4\n")
				cause = nil
			case testParseMode:
				file.Reader = strings.NewReader("cpu 1 invalid 3 4\n")
				cause = nil
			case testOversizedMode:
				file.Reader = strings.NewReader("cpu 1 2 3 4" + strings.Repeat(" ", 8192))
				cause = nil
			}
			if mode != testMissingMode && mode != testOpenMode {
				source.procFS = cpuFS(func(name string) (fs.File, error) {
					if name != testStatName {
						t.Fatalf("unexpected path %s", name)
					}
					return file, nil
				})
			}
			got, err := source.Collect(context.Background())
			if err == nil || got != (metrics.CPUCounters{}) {
				t.Fatalf("%+v %v", got, err)
			}
			if cause != nil && !errors.Is(err, cause) {
				t.Fatalf("lost cause: %v", err)
			}
			if mode != testMissingMode && mode != testOpenMode && !file.closed {
				t.Fatal("file not closed")
			}
		})
	}
}

func TestCPUSourceSizeBoundary(t *testing.T) {
	for _, newline := range []bool{false, true} {
		for _, size := range []int{4096, 4097} {
			input := "cpu 1 2 3 4"
			suffix := ""
			if newline {
				suffix = "\n"
			}
			input += strings.Repeat(" ", size-len(input)-len(suffix)) + suffix
			source := NewCPUSource()
			source.procFS = fstest.MapFS{testStatName: &fstest.MapFile{Data: []byte(input)}}
			got, err := source.Collect(context.Background())
			if size == 4096 {
				if err != nil || got.Idle != 4 {
					t.Fatalf("size %d newline=%v: %+v %v", size, newline, got, err)
				}
			} else if err == nil {
				t.Fatal("oversized line accepted")
			}
		}
	}
}

func TestCPUSourceBoundsRead(t *testing.T) {
	for _, newline := range []bool{false, true} {
		input := "cpu 1 2 3 4"
		if newline {
			input += "\n"
		}
		reader := strings.NewReader(input + strings.Repeat(" ", 8192))
		consumed := 0
		file := &cpuFile{
			Reader: cpuReader(
				func(p []byte) (int, error) { n, err := reader.Read(p); consumed += n; return n, err },
			),
		}
		source := NewCPUSource()
		source.procFS = cpuFS(func(string) (fs.File, error) { return file, nil })
		_, err := source.Collect(context.Background())
		if newline && err != nil {
			t.Fatal(err)
		}
		if !newline && err == nil {
			t.Fatal("oversized line accepted")
		}
		if consumed > 4097 || !file.closed {
			t.Fatalf("consumed %d, closed=%v", consumed, file.closed)
		}
	}
}

func TestCPUSourceCancellation(t *testing.T) {
	t.Run("before open", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		source := NewCPUSource()
		source.procFS = cpuFS(
			func(string) (fs.File, error) { t.Fatal("opened after cancel"); return nil, nil },
		)
		got, err := source.Collect(ctx)
		if !errors.Is(err, context.Canceled) || got != (metrics.CPUCounters{}) {
			t.Fatalf("%+v %v", got, err)
		}
	})
	t.Run("during read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		file := &cpuFile{
			Reader: cpuReader(
				func(p []byte) (int, error) { cancel(); return copy(p, "cpu 1 2 3 4\n"), io.EOF },
			),
		}
		source := NewCPUSource()
		source.procFS = cpuFS(func(string) (fs.File, error) { return file, nil })
		got, err := source.Collect(ctx)
		if !errors.Is(err, context.Canceled) || got != (metrics.CPUCounters{}) || !file.closed {
			t.Fatalf("%+v %v, closed=%v", got, err, file.closed)
		}
	})
}

func TestCPUSourceReadErrorAfterCompleteLine(t *testing.T) {
	sentinel := errors.New("read failed with complete line")
	file := &cpuFile{
		Reader: cpuReader(
			func(p []byte) (int, error) { return copy(p, "cpu 1 2 3 4\n"), sentinel },
		),
	}
	source := NewCPUSource()
	source.procFS = cpuFS(func(string) (fs.File, error) { return file, nil })
	got, err := source.Collect(context.Background())
	if !errors.Is(err, sentinel) || got != (metrics.CPUCounters{}) || !file.closed {
		t.Fatalf("got %+v, error %v, closed=%v", got, err, file.closed)
	}
}
