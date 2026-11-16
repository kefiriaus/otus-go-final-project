package collector

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const testOtherDiskIdentity = "8:16"

func TestParseDiskIOStat(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  metrics.DiskIOCounters
	}{
		{
			"10 99 20 88 5 77 10 66 55 44 33",
			metrics.DiskIOCounters{ReadsCompleted: 10, WritesCompleted: 5, SectorsRead: 20, SectorsWritten: 10},
		},
		{
			"\t10 99 20 88 5 77 10 66 55 44 33 100 200 300 400 500 600\n",
			metrics.DiskIOCounters{ReadsCompleted: 10, WritesCompleted: 5, SectorsRead: 20, SectorsWritten: 10},
		},
		{"0 0 0 0 0 0 0 0 0 0 0", metrics.DiskIOCounters{}},
		{
			"18446744073709551615 0 18446744073709551614 0 1 0 2 0 0 0 0",
			metrics.DiskIOCounters{
				ReadsCompleted: math.MaxUint64, WritesCompleted: 1,
				SectorsRead: math.MaxUint64 - 1, SectorsWritten: 2,
			},
		},
	} {
		got, err := parseDiskIOStat([]byte(tc.input))
		if err != nil || got != tc.want {
			t.Fatalf("got %+v %v want %+v", got, err, tc.want)
		}
	}
	for _, input := range []string{"", "1 2 3 4 5 6 7 8 9 10"} {
		got, err := parseDiskIOStat([]byte(input))
		if err == nil || got != (metrics.DiskIOCounters{}) {
			t.Fatalf("%+v %v", got, err)
		}
	}
	for i := 0; i < 17; i++ {
		for _, bad := range []string{testBadValue, "-1", "1.5", "18446744073709551616"} {
			t.Run(fmt.Sprintf("field%d/%s", i, bad), func(t *testing.T) {
				fields := strings.Fields("1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17")
				fields[i] = bad
				got, err := parseDiskIOStat([]byte(strings.Join(fields, " ")))
				var number *strconv.NumError
				if err == nil || got != (metrics.DiskIOCounters{}) || !errors.As(err, &number) {
					t.Fatalf("got %+v, lost numeric cause: %v", got, err)
				}
			})
		}
	}
}

func TestParseDiskDeviceIdentity(t *testing.T) {
	for _, tc := range [][2]string{
		{"259:0\n", "259:0"},
		{testOtherDiskIdentity, testOtherDiskIdentity},
		{" 008:016\n", testOtherDiskIdentity},
		{"4294967295:4294967295", "4294967295:4294967295"},
	} {
		got, err := parseDiskDeviceIdentity([]byte(tc[0]))
		if err != nil || got != tc[1] {
			t.Fatalf("%q %v", got, err)
		}
	}
	for _, input := range []string{
		"", ":", "1:", ":1", "1:2:3", "-1:0", "+1:0",
		"1:+0", "1.0:0", "1:4294967296", "4294967296:0", "x:0", "1 :0",
	} {
		got, err := parseDiskDeviceIdentity([]byte(input))
		if err == nil || got != "" {
			t.Fatalf("accepted %q: %q %v", input, got, err)
		}
	}
}
