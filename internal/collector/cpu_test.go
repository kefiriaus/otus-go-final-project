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

func TestParseCPUStatLine(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  metrics.CPUCounters
	}{
		{"cpu 20 10 15 40 5 3 2 5 100 200\n", metrics.CPUCounters{User: 20, Nice: 10, System: 15, Idle: 40, IOWait: 5, IRQ: 3, SoftIRQ: 2, Steal: 5}},
		{"\tcpu \t1 2 3 4", metrics.CPUCounters{User: 1, Nice: 2, System: 3, Idle: 4}},
		{"cpu 0 0 0 0 0 0 0 0 0 0", metrics.CPUCounters{}},
		{"cpu 1 2 3 4 5 6 7 8 9 10 11", metrics.CPUCounters{User: 1, Nice: 2, System: 3, Idle: 4, IOWait: 5, IRQ: 6, SoftIRQ: 7, Steal: 8}},
		{"cpu 18446744073709551615 0 0 18446744073709551614", metrics.CPUCounters{User: math.MaxUint64, Idle: math.MaxUint64 - 1}},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseCPUStatLine([]byte(tc.input))
			if err != nil || got != tc.want {
				t.Fatalf("got %+v %v, want %+v", got, err, tc.want)
			}
		})
	}
}
func TestParseCPUStatLineInvalid(t *testing.T) {
	for _, input := range []string{"", "cpu", "cpu 1 2 3", "cpu0 1 2 3 4", "intr 1 2 3 4"} {
		if got, err := parseCPUStatLine([]byte(input)); err == nil || got != (metrics.CPUCounters{}) {
			t.Fatalf("accepted %q: %+v %v", input, got, err)
		}
	}
	for i := 0; i < 11; i++ {
		for _, bad := range []string{"bad", "-1", "1.5", "18446744073709551616"} {
			t.Run(fmt.Sprintf("field%d/%s", i, bad), func(t *testing.T) {
				fields := strings.Fields("cpu 1 2 3 4 5 6 7 8 9 10 11")
				fields[i+1] = bad
				got, err := parseCPUStatLine([]byte(strings.Join(fields, " ")))
				var number *strconv.NumError
				if err == nil || got != (metrics.CPUCounters{}) || !errors.As(err, &number) {
					t.Fatalf("got %+v, lost parse error: %v", got, err)
				}
			})
		}
	}
}
