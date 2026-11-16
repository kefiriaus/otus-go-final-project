package collector

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

const (
	testCommName            = "comm"
	testOwnerMode           = "owner"
	testDisappearMode       = "disappear"
	testStatusName          = "status"
	testOversizedCommMode   = "oversized comm"
	testOversizedStatusMode = "oversized status"
)

func ownerFixture() fstest.MapFS {
	result := fstest.MapFS{}
	for _, pid := range []int{42, 43} {
		base := fmt.Sprint(pid)
		result[base+"/fd/3"] = &fstest.MapFile{Mode: fs.ModeSymlink, Data: []byte("socket:[123]")}
		result[base+"/comm"] = &fstest.MapFile{Data: []byte(" worker \n")}
		result[base+"/status"] = &fstest.MapFile{
			Data: []byte(fmt.Sprintf("Uid:\t9999 %d 9999 9999\n", pid)),
		}
	}
	result["42/fd/4"] = &fstest.MapFile{Mode: fs.ModeSymlink, Data: []byte("socket:[123]")}
	result["42/fd/5"] = &fstest.MapFile{Mode: fs.ModeSymlink, Data: []byte("pipe:[456]")}
	return result
}

type ownerTestFS struct {
	fs.FS
	open func(string) (fs.File, error)
	link func(string) (string, error)
}

func (f ownerTestFS) Open(name string) (fs.File, error) {
	if f.open != nil {
		return f.open(name)
	}
	return f.FS.Open(name)
}

func (f ownerTestFS) ReadLink(name string) (string, error) {
	if f.link != nil {
		return f.link(name)
	}
	return fs.ReadLink(f.FS, name)
}
func (f ownerTestFS) Lstat(name string) (fs.FileInfo, error) { return fs.Lstat(f.FS, name) }
func TestConnectionOwners(t *testing.T) {
	source, err := NewConnectionsSource(DefaultConnectionsOptions())
	if err != nil {
		t.Fatal(err)
	}
	source.procFS = ownerFixture()
	owners, issues := source.lookupOwners(context.Background(), []uint64{123})
	if issues.partial || len(owners[123]) != 2 {
		t.Fatalf("%+v %+v", owners, issues)
	}
	for i, owner := range owners[123] {
		want := uint32(42 + i)
		if owner.PID == nil || *owner.PID != want || owner.Command == nil ||
			*owner.Command != " worker " ||
			owner.User == nil ||
			*owner.User != fmt.Sprint(want) {
			t.Fatalf("%+v", owner)
		}
	}
	source.procFS = ownerTestFS{
		FS:   ownerFixture(),
		open: func(string) (fs.File, error) { t.Fatal("scanned without targets"); return nil, nil },
	}
	empty, issues := source.lookupOwners(context.Background(), nil)
	if len(empty) != 0 || issues.partial {
		t.Fatalf("%v %+v", empty, issues)
	}
}

func configureOwnerPartial(
	source *ConnectionsSource,
	fixture fstest.MapFS,
	mode string,
	sentinel error,
) uint64 {
	source.procFS = fixture
	switch mode {
	case "root":
		source.procFS = ownerTestFS{
			FS: fixture, open: func(string) (fs.File, error) { return nil, sentinel },
		}
	case "fd":
		source.procFS = ownerTestFS{FS: fixture, open: func(name string) (fs.File, error) {
			if strings.HasSuffix(name, "/fd") {
				return nil, sentinel
			}
			return fixture.Open(name)
		}}
	case testLinkMode:
		source.procFS = ownerTestFS{
			FS: fixture, link: func(string) (string, error) { return "", sentinel },
		}
	case testDisappearMode:
		source.procFS = ownerTestFS{
			FS: fixture, link: func(string) (string, error) { return "", fs.ErrNotExist },
		}
	case testCommName:
		delete(fixture, "42/comm")
	case testStatusName:
		fixture["42/status"].Data = []byte("bad status")
	case "badlink":
		fixture["42/fd/3"].Data = []byte("socket:[bad]")
	case "oversized link":
		fixture["42/fd/3"].Data = []byte(strings.Repeat("x", 4097))
	case testOversizedCommMode:
		fixture["42/comm"].Data = []byte(strings.Repeat("x", 257))
	case testOversizedStatusMode:
		fixture["42/status"].Data = []byte(strings.Repeat("x", 65537))
	case "unmatched":
		return 999
	case "no readlink":
		source.procFS = struct{ fs.FS }{fixture}
	}
	return 123
}

func TestConnectionOwnersPartial(t *testing.T) {
	sentinel := errors.New("permission denied")
	for _, mode := range []string{
		"root", "fd", testLinkMode, testDisappearMode, testCommName, testStatusName,
		"badlink", "oversized link", testOversizedCommMode, testOversizedStatusMode,
		"unmatched", "no readlink",
	} {
		t.Run(mode, func(t *testing.T) {
			fixture := ownerFixture()
			source, _ := NewConnectionsSource(DefaultConnectionsOptions())
			target := configureOwnerPartial(source, fixture, mode, sentinel)
			owners, issues := source.lookupOwners(context.Background(), []uint64{target})
			if !issues.partial || issues.detail == "" || len(issues.detail) > 4096 {
				t.Fatalf("%+v %+v", owners, issues)
			}
			if mode == testCommName || mode == testOversizedCommMode {
				if len(owners[123]) != 2 || owners[123][0].PID == nil ||
					owners[123][0].Command != nil {
					t.Fatal("lost PID or fabricated command")
				}
			}
			if mode == testStatusName || mode == testOversizedStatusMode {
				if len(owners[123]) != 2 || owners[123][0].User != nil {
					t.Fatal("fabricated user")
				}
			}
		})
	}
}

func TestConnectionOwnerLimits(t *testing.T) {
	for _, mode := range []string{"process", "FD", "per process FD", testOwnerMode, "budget", "reserve"} {
		t.Run(mode, func(t *testing.T) {
			o := DefaultConnectionsOptions()
			switch mode {
			case "process":
				o.MaxProcesses = 1
			case "FD":
				o.MaxFDs = 1
			case "per process FD":
				o.MaxFDsPerProcess = 1
			case testOwnerMode:
				o.MaxOwnersPerSocket = 1
			case "budget":
				o.OwnerTimeout = time.Nanosecond
			}
			source, err := NewConnectionsSource(o)
			if err != nil {
				t.Fatal(err)
			}
			source.procFS = ownerFixture()
			ctx := context.Background()
			var cancel context.CancelFunc
			if mode == "reserve" {
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
			}
			owners, issues := source.lookupOwners(ctx, []uint64{123})
			if !issues.partial {
				t.Fatalf("%s: %+v %+v", mode, owners, issues)
			}
			if mode == testOwnerMode && len(owners[123]) != 1 {
				t.Fatal(owners)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source, _ := NewConnectionsSource(DefaultConnectionsOptions())
	source.procFS = ownerFixture()
	_, issues := source.lookupOwners(ctx, []uint64{123})
	if !issues.partial {
		t.Fatal("ignored cancellation")
	}
}

type ownerTrackedDirectory struct {
	fs.ReadDirFile
	closed bool
}

func (d *ownerTrackedDirectory) Close() error { d.closed = true; return d.ReadDirFile.Close() }
func TestConnectionOwnersReadLinksBeforeDirectoryClose(t *testing.T) {
	fixture := ownerFixture()
	directories := make(map[string]*ownerTrackedDirectory)
	source, _ := NewConnectionsSource(DefaultConnectionsOptions())
	source.procFS = ownerTestFS{FS: fixture, open: func(name string) (fs.File, error) {
		file, err := fixture.Open(name)
		if err == nil && strings.HasSuffix(name, "/fd") {
			directory := &ownerTrackedDirectory{ReadDirFile: file.(fs.ReadDirFile)}
			directories[name] = directory
			return directory, nil
		}
		return file, err
	}, link: func(name string) (string, error) {
		directory := directories[name[:strings.LastIndex(name, "/")]]
		if directory == nil || directory.closed {
			return "", fs.ErrNotExist
		}
		return fixture.ReadLink(name)
	}}
	owners, issues := source.lookupOwners(context.Background(), []uint64{123})
	if issues.partial || len(owners[123]) != 2 {
		t.Fatalf("read links after closing their directory: %+v %+v", owners, issues)
	}
	for _, directory := range directories {
		if !directory.closed {
			t.Fatal("directory leaked")
		}
	}
}

func TestConnectionOwnerRootEntryLimit(t *testing.T) {
	o := DefaultConnectionsOptions()
	o.MaxProcesses = 1
	source, _ := NewConnectionsSource(o)
	fixture := fstest.MapFS{}
	for i := 0; i < 515; i++ {
		fixture[fmt.Sprintf("metadata%04d", i)] = &fstest.MapFile{}
	}
	source.procFS = fixture
	_, issues := source.lookupOwners(context.Background(), []uint64{123})
	if !issues.partial || !strings.Contains(issues.detail, "entry scan limit") {
		t.Fatalf("unbounded non-PID scan: %+v", issues)
	}
}
