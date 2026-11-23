package collector

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

const testFilesystemIdentity = "8:0"

func mountLine(id, parent int, mount, kind, device string) string {
	return fmt.Sprintf(
		"%d %d 8:0 / %s rw shared:1 unknown:tag - %s %s rw\n",
		id,
		parent,
		mount,
		kind,
		device,
	)
}

func TestParseMountInfo(t *testing.T) {
	input := mountLine(
		1,
		0,
		"/",
		"ext4",
		"/dev/sda",
	) + mountLine(
		2,
		1,
		`/with\040space\011tab\012line\134slash`,
		"xfs",
		`/dev/a\040b`,
	)
	got, err := parseMountInfo([]byte(input))
	if err != nil || len(got) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	if got[1].MountPoint != "/with space\ttab\nline\\slash" || got[1].Device != "/dev/a b" ||
		got[1].ID != 2 ||
		got[1].ParentID != 1 ||
		got[1].MajorMinor != testFilesystemIdentity {
		t.Fatal(got[1])
	}
	inputs := []string{
		"invalid",
		mountLine(1, 0, "relative", "ext4", "x"),
		mountLine(1, 0, `/bad\777`, "ext4", "x"),
		mountLine(1, 0, `/bad\000`, "ext4", "x"),
		mountLine(1, 0, `/bad\`, "ext4", "x"),
		mountLine(1, 0, "/", "ext4", "x") + mountLine(1, 0, "/a", "ext4", "x"),
		strings.Replace(mountLine(1, 0, "/", "ext4", "x"), testFilesystemIdentity, "invalid", 1),
		strings.Replace(mountLine(1, 0, "/", "ext4", "x"), " - ", " missing ", 1),
		strings.Replace(mountLine(1, 0, "/", "ext4", "x"), "1 0", "x 0", 1),
	}
	for _, bad := range inputs {
		if _, err := parseMountInfo([]byte(bad)); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	if _, err := parseMountInfo([]byte(strings.Repeat("x", (1<<20)+1))); err == nil {
		t.Fatal("byte bound")
	}
	if _, err := parseMountInfo(
		[]byte(mountLine(1, 0, "/", "ext4", strings.Repeat("x", 1<<16))),
	); err == nil {
		t.Fatal("line bound")
	}
	var many strings.Builder
	for i := 1; i <= 1025; i++ {
		many.WriteString(mountLine(i, 0, fmt.Sprintf("/m%d", i), "ext4", "x"))
	}
	if _, err := parseMountInfo([]byte(many.String())); err == nil {
		t.Fatal("record bound")
	}
}

func TestVisibleFilesystemMounts(t *testing.T) {
	input := mountLine(
		1,
		0,
		"/",
		"ext4",
		"root",
	) + mountLine(
		2,
		1,
		"/mnt",
		"ext4",
		"old",
	) + mountLine(
		3,
		2,
		"/mnt/sub",
		"xfs",
		"hidden",
	) + mountLine(
		4,
		2,
		"/mnt",
		"xfs",
		"new",
	) + mountLine(
		5,
		4,
		"/mnt/visible",
		"ext4",
		"bind",
	) + mountLine(
		6,
		1,
		"/proc",
		"proc",
		"proc",
	) + mountLine(
		7,
		1,
		"/virtual",
		"overlay",
		"overlay",
	)
	mounts, err := parseMountInfo([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	got, err := visibleFilesystemMounts(mounts)
	if err != nil || len(got) != 3 || got[1].Device != "new" ||
		got[2].MountPoint != "/mnt/visible" {
		t.Fatalf("%+v %v", got, err)
	}
	covered, _ := parseMountInfo(
		[]byte(mountLine(1, 0, "/", "ext4", "old") + mountLine(2, 1, "/", "tmpfs", "new")),
	)
	got, err = visibleFilesystemMounts(covered)
	if err != nil || len(got) != 0 {
		t.Fatalf("hidden mount leaked: %+v %v", got, err)
	}
	inputs := []string{
		mountLine(1, 0, "/", "ext4", "x") + mountLine(2, 0, "/", "ext4", "y"),
		mountLine(1, 2, "/a", "ext4", "x") + mountLine(2, 1, "/b", "ext4", "y"),
	}
	for _, input := range inputs {
		mounts, _ := parseMountInfo([]byte(input))
		if _, err := visibleFilesystemMounts(mounts); err == nil {
			t.Fatal("ambiguous stack/cycle")
		}
	}
	var many strings.Builder
	for i := 1; i <= 129; i++ {
		many.WriteString(mountLine(i, 0, fmt.Sprintf("/m%d", i), "ext4", "x"))
	}
	mounts, _ = parseMountInfo([]byte(many.String()))
	if _, err := visibleFilesystemMounts(mounts); err == nil {
		t.Fatal("mount bound")
	}
	for _, kind := range []string{"tmpfs", "sysfs", "devtmpfs", "cgroup2", "fusectl", "ramfs", "aufs", "autofs", "bpf"} {
		mounts, _ := parseMountInfo(
			[]byte(
				mountLine(1, 0, "/", "ext4", "disk") + mountLine(2, 1, "/service", kind, "none"),
			),
		)
		got, err := visibleFilesystemMounts(mounts)
		if err != nil || len(got) != 1 {
			t.Fatalf("kind %s: %+v %v", kind, got, err)
		}
	}
}

func TestFilesystemUsage(t *testing.T) {
	mounts, _ := parseMountInfo([]byte(mountLine(1, 0, "/", "ext4", "/dev/sda")))
	m := mounts[0]
	got, err := filesystemUsage(
		m,
		filesystemStats{
			BlockSize:   4096,
			Blocks:      1024,
			FreeBlocks:  256,
			Files:       100,
			FreeFiles:   25,
			InodesKnown: true,
		},
	)
	if err != nil || got.UsedMebibytes != 3 || got.UsedPercent == nil || *got.UsedPercent != 75 ||
		got.UsedInodes == nil ||
		*got.UsedInodes != 75 ||
		*got.UsedInodesPercent != 75 ||
		got.MountPoint != "/" ||
		got.Identity == "" {
		t.Fatalf("%+v %v", got, err)
	}
	zero, err := filesystemUsage(m, filesystemStats{BlockSize: 4096})
	if err != nil || zero.UsedMebibytes != 0 || zero.UsedPercent != nil || zero.UsedInodes != nil ||
		zero.UsedInodesPercent != nil {
		t.Fatalf("%+v %v", zero, err)
	}
	huge, err := filesystemUsage(
		m,
		filesystemStats{BlockSize: math.MaxUint64, Blocks: math.MaxUint64},
	)
	if err != nil || math.IsInf(huge.UsedMebibytes, 0) || huge.UsedMebibytes <= 0 {
		t.Fatalf("%+v %v", huge, err)
	}
	for _, bad := range []filesystemStats{
		{Blocks: 1},
		{BlockSize: 4096, Blocks: 1, FreeBlocks: 2},
		{BlockSize: 4096, Files: 1, FreeFiles: 2, InodesKnown: true},
	} {
		if _, err := filesystemUsage(m, bad); err == nil {
			t.Fatal("invalid statistics")
		}
	}
}

func TestParseMountInfoNamespaceRoot(t *testing.T) {
	mounts, err := parseMountInfo([]byte("1 0 0:4 net:[42] /run/netns/id rw - nsfs nsfs rw\n"))
	if err != nil || len(mounts) != 1 || mounts[0].Root != "net:[42]" {
		t.Fatalf("%+v %v", mounts, err)
	}
	selected, err := visibleFilesystemMounts(mounts)
	if err != nil || len(selected) != 0 {
		t.Fatalf("namespace mount leaked: %+v %v", selected, err)
	}
}

func TestVisibleFilesystemMountsHiddenDuplicateChildren(t *testing.T) {
	input := mountLine(
		1,
		0,
		"/",
		"ext4",
		"root",
	) + mountLine(
		2,
		1,
		"/branch",
		"ext4",
		"oldparent",
	) + mountLine(
		3,
		2,
		"/branch/sub",
		"ext4",
		"oldchild",
	) + mountLine(
		4,
		2,
		"/branch",
		"tmpfs",
		"newparent",
	) + mountLine(
		5,
		4,
		"/branch/sub",
		"ext4",
		"newchild",
	)
	mounts, err := parseMountInfo([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	got, err := visibleFilesystemMounts(mounts)
	if err != nil || len(got) != 2 || got[1].Device != "newchild" {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestParseMountInfoUnicodeWhitespace(t *testing.T) {
	mount := "/data\u00a0volume"
	got, err := parseMountInfo([]byte(mountLine(1, 0, mount, "ext4", "disk")))
	if err != nil || len(got) != 1 || got[0].MountPoint != mount {
		t.Fatalf("%+v %v", got, err)
	}
}
