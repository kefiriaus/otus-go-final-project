package collector

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
)

const testLoadavgName = "loadavg"

type loadAverageFS func(string) (fs.File, error)

func (f loadAverageFS) Open(name string) (fs.File, error) { return f(name) }

type loadAverageFile struct {
	io.Reader
	closed   bool
	closeErr error
}

func (f *loadAverageFile) Stat() (fs.FileInfo, error) { return nil, errors.New("unexpected Stat") }
func (f *loadAverageFile) Close() error               { f.closed = true; return f.closeErr }

type loadAverageReader func([]byte) (int, error)

func (f loadAverageReader) Read(p []byte) (int, error) { return f(p) }
func TestLoadAverageSource(t *testing.T) {
	for _, input := range []string{"1.25 0.50 2.75 2/100 12345\n", "0 0 0"} {
		source := NewLoadAverageSource()
		source.procFS = fstest.MapFS{testLoadavgName: &fstest.MapFile{Data: []byte(input)}}
		got, err := source.Collect(context.Background())
		want, _ := parseLoadAverage([]byte(input))
		if err != nil || got != want {
			t.Fatalf("got %+v, %v want %+v", got, err, want)
		}
	}
}

func TestLoadAverageSourceErrors(t *testing.T) {
	sentinel := errors.New("read failed")
	for _, mode := range []string{
		testMissingMode, testOpenMode, testReadMode, testCloseMode, testParseMode, testOversizedMode,
	} {
		t.Run(mode, func(t *testing.T) {
			source := NewLoadAverageSource()
			file := &loadAverageFile{Reader: strings.NewReader("1 2 3")}
			cause := sentinel
			switch mode {
			case testMissingMode:
				source.procFS = fstest.MapFS{}
				cause = fs.ErrNotExist
			case testOpenMode:
				source.procFS = loadAverageFS(
					func(string) (fs.File, error) { return nil, sentinel },
				)
			case testReadMode:
				file.Reader = loadAverageReader(
					func(p []byte) (int, error) { return copy(p, "1 2 3"), sentinel },
				)
			case testCloseMode:
				file.closeErr = sentinel
			case testParseMode:
				file.Reader = strings.NewReader("1 invalid 3")
				cause = nil
			case testOversizedMode:
				file.Reader = strings.NewReader("1 2 3" + strings.Repeat(" ", 4097))
				cause = nil
			}
			if mode != testMissingMode && mode != testOpenMode {
				source.procFS = loadAverageFS(func(name string) (fs.File, error) {
					if name != testLoadavgName {
						t.Fatalf("unexpected path %s", name)
					}
					return file, nil
				})
			}
			got, err := source.Collect(context.Background())
			if err == nil || got.Load1 != 0 || got.Load5 != 0 || got.Load15 != 0 {
				t.Fatalf("got %+v %v", got, err)
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

func TestLoadAverageSourceSizeBoundary(t *testing.T) {
	for _, size := range []int{4096, 4097} {
		source := NewLoadAverageSource()
		source.procFS = fstest.MapFS{
			testLoadavgName: &fstest.MapFile{Data: []byte("1 2 3" + strings.Repeat(" ", size-5))},
		}
		got, err := source.Collect(context.Background())
		if size == 4096 {
			if err != nil || got.Load15 != 3 {
				t.Fatalf("%+v %v", got, err)
			}
		} else if err == nil {
			t.Fatal("oversized input accepted")
		}
	}
}

func TestLoadAverageSourceCancellation(t *testing.T) {
	t.Run("before open", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		source := NewLoadAverageSource()
		source.procFS = loadAverageFS(
			func(string) (fs.File, error) { t.Fatal("opened after cancellation"); return nil, nil },
		)
		got, err := source.Collect(ctx)
		if !errors.Is(err, context.Canceled) || got.Load1 != 0 {
			t.Fatalf("%+v %v", got, err)
		}
	})
	t.Run("during read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		file := &loadAverageFile{
			Reader: loadAverageReader(
				func(p []byte) (int, error) { cancel(); return copy(p, "1 2 3"), io.EOF },
			),
		}
		source := NewLoadAverageSource()
		source.procFS = loadAverageFS(func(string) (fs.File, error) { return file, nil })
		got, err := source.Collect(ctx)
		if !errors.Is(err, context.Canceled) || got.Load1 != 0 || !file.closed {
			t.Fatalf("%+v %v closed=%v", got, err, file.closed)
		}
	})
}

func TestLoadAverageSourceBoundsRead(t *testing.T) {
	reader := strings.NewReader("1 2 3" + strings.Repeat(" ", 8192))
	consumed := 0
	file := &loadAverageFile{
		Reader: loadAverageReader(
			func(p []byte) (int, error) { n, err := reader.Read(p); consumed += n; return n, err },
		),
	}
	source := NewLoadAverageSource()
	source.procFS = loadAverageFS(func(string) (fs.File, error) { return file, nil })
	if _, err := source.Collect(context.Background()); err == nil {
		t.Fatal("oversized input accepted")
	}
	if consumed != 4097 || !file.closed {
		t.Fatalf("consumed %d bytes, closed=%v", consumed, file.closed)
	}
}
