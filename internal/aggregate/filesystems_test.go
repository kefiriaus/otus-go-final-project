package aggregate

import (
	"math"
	"testing"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func fsNumber(v float64) *float64 { return &v }
func fsUsage(identity string, used, percentage, inodes float64) metrics.FilesystemUsage {
	return metrics.FilesystemUsage{
		MountPoint:        "/data",
		Device:            "disk",
		FilesystemType:    "ext4",
		Identity:          identity,
		UsedMebibytes:     used,
		UsedPercent:       fsNumber(percentage),
		UsedInodes:        fsNumber(inodes),
		UsedInodesPercent: fsNumber(inodes),
	}
}

func TestMeanFilesystemUsage(t *testing.T) {
	values := []Interval[metrics.FilesystemUsage]{
		interval(0, 1, fsUsage("first", 3, 75, 1)),
		interval(1, 2, fsUsage("first", 4, 25, 2)),
	}
	got, err := MeanFilesystemUsage(Window{at(0), at(2)}, "/data", values)
	if err != nil || got.Data == nil || got.Partial || got.CoverageRatio != 1 ||
		!got.LastSampleAt.Equal(at(1)) {
		t.Fatalf("%+v %v", got, err)
	}
	closeTo(t, got.Data.UsedMebibytes, 3.5)
	closeTo(t, *got.Data.UsedPercent, 50)
	closeTo(t, *got.Data.UsedInodes, 1.5)
	clipped, err := MeanFilesystemUsage(Window{at(.5), at(1.5)}, "/data", values)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, clipped.Data.UsedMebibytes, 3.5)
	empty, err := MeanFilesystemUsage(Window{at(3), at(4)}, "/data", values)
	if err != nil || empty.Data != nil || empty.LastSampleAt != nil || empty.Partial ||
		empty.CoverageRatio != 0 {
		t.Fatalf("%+v %v", empty, err)
	}
	zero := fsUsage("first", 0, 0, 0)
	measured, err := MeanFilesystemUsage(
		Window{at(0), at(1)},
		"/data",
		[]Interval[metrics.FilesystemUsage]{interval(0, 1, zero)},
	)
	if err != nil || measured.Data == nil || measured.Data.UsedMebibytes != 0 ||
		measured.Data.UsedPercent == nil ||
		*measured.Data.UsedPercent != 0 {
		t.Fatalf("%+v %v", measured, err)
	}
	(*got.Data.UsedPercent) = 999
	if *values[0].Value.UsedPercent != 75 {
		t.Fatal("aliased caller optional field")
	}
}

func TestMeanFilesystemUsageOptional(t *testing.T) {
	unknown := fsUsage("first", 0, 0, 0)
	unknown.UsedPercent = nil
	unknown.UsedInodes = nil
	unknown.UsedInodesPercent = nil
	got, err := MeanFilesystemUsage(
		Window{at(0), at(1)},
		"/data",
		[]Interval[metrics.FilesystemUsage]{interval(0, 1, unknown)},
	)
	if err != nil || got.Data == nil || got.Partial || got.Data.UsedPercent != nil ||
		got.Data.UsedInodes != nil ||
		got.Data.UsedInodesPercent != nil {
		t.Fatalf("%+v %v", got, err)
	}
	mixed, err := MeanFilesystemUsage(
		Window{at(0), at(2)},
		"/data",
		[]Interval[metrics.FilesystemUsage]{
			interval(0, 1, unknown),
			interval(1, 2, fsUsage("first", 4, 25, 2)),
		},
	)
	if err != nil || !mixed.Partial || mixed.CoverageRatio != 1 {
		t.Fatalf("%+v %v", mixed, err)
	}
	closeTo(t, *mixed.Data.UsedPercent, 25)
	closeTo(t, *mixed.Data.UsedInodes, 2)
	partial := interval(0, 1, fsUsage("first", 1, 25, 1))
	partial.Partial = true
	p, err := MeanFilesystemUsage(
		Window{at(0), at(1)},
		"/data",
		[]Interval[metrics.FilesystemUsage]{partial},
	)
	if err != nil || !p.Partial {
		t.Fatalf("%+v %v", p, err)
	}
}

func TestMeanFilesystemUsageIdentity(t *testing.T) {
	old := fsUsage("old", 100, 100, 100)
	newUsage := fsUsage("new", 1, 10, 1)
	newUsage.Device = "newdisk"
	newUsage.FilesystemType = "xfs"
	got, err := MeanFilesystemUsage(
		Window{at(0), at(2)},
		"/data",
		[]Interval[metrics.FilesystemUsage]{interval(0, 1, old), interval(1, 2, newUsage)},
	)
	if err != nil || got.Data == nil || got.Data.Device != "newdisk" ||
		got.Data.Identity != "new" ||
		got.CoverageRatio != .5 ||
		!got.Partial {
		t.Fatalf("%+v %v", got, err)
	}
	closeTo(t, got.Data.UsedMebibytes, 1)
}

func TestMeanFilesystemUsageInvalid(t *testing.T) {
	if _, err := MeanFilesystemUsage(Window{}, "/data", nil); err == nil {
		t.Fatal("invalid window")
	}
	if _, err := MeanFilesystemUsage(Window{at(0), at(1)}, "relative", nil); err == nil {
		t.Fatal("relative mount")
	}
	for _, mode := range []string{
		"mount", "identity", "device", "kind", "negative", "nan",
		"infinity", "percent", "inodes", "inode percent", "orphan percent",
	} {
		v := fsUsage("first", 1, 25, 1)
		switch mode {
		case "mount":
			v.MountPoint = "/other"
		case "identity":
			v.Identity = ""
		case "device":
			v.Device = ""
		case "kind":
			v.FilesystemType = ""
		case "negative":
			v.UsedMebibytes = -1
		case "nan":
			v.UsedMebibytes = math.NaN()
		case "infinity":
			v.UsedPercent = fsNumber(math.Inf(1))
		case "percent":
			v.UsedPercent = fsNumber(101)
		case "inodes":
			v.UsedInodes = fsNumber(-1)
		case "inode percent":
			v.UsedInodesPercent = fsNumber(101)
		case "orphan percent":
			v.UsedInodes = nil
		}
		got, err := MeanFilesystemUsage(
			Window{at(0), at(1)},
			"/data",
			[]Interval[metrics.FilesystemUsage]{interval(0, 1, v)},
		)
		if err == nil || got.Data != nil {
			t.Fatalf("accepted %s: %+v", mode, got)
		}
	}
	a, b := fsUsage("first", 1, 25, 1), fsUsage("first", 1, 25, 1)
	b.Device = "different"
	if _, err := MeanFilesystemUsage(
		Window{at(0), at(2)},
		"/data",
		[]Interval[metrics.FilesystemUsage]{interval(0, 1, a), interval(1, 2, b)},
	); err == nil {
		t.Fatal("metadata changed within identity")
	}
	if _, err := MeanFilesystemUsage(
		Window{at(0), at(2)},
		"/data",
		[]Interval[metrics.FilesystemUsage]{interval(0, 2, a), interval(1, 2, a)},
	); err == nil {
		t.Fatal("overlap")
	}
}
