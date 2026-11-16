package collector

import (
	"errors"
	"strconv"
	"testing"
)

func TestParseLoadAverage(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  [3]float64
	}{
		{"1.25 0.50 2.75 2/100 12345\n", [3]float64{1.25, .5, 2.75}},
		{"\t1.25 \n 0.50\t2.75   ignored", [3]float64{1.25, .5, 2.75}},
		{"0 0 0", [3]float64{}},
		{"1000000 2000000 3000000", [3]float64{1e6, 2e6, 3e6}},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseLoadAverage([]byte(tc.input))
			if err != nil {
				t.Fatal(err)
			}
			if [3]float64{got.Load1, got.Load5, got.Load15} != tc.want {
				t.Fatalf("got %+v want %v", got, tc.want)
			}
		})
	}
}

func TestParseLoadAverageInvalid(t *testing.T) {
	for _, input := range []string{
		"", " ", "1", "1 2", "x 2 3", "1 x 3", "1 2 x",
		"-1 2 3", "1 -2 3", "1 2 -3", "NaN 2 3", "1 +Inf 3", "1 2 -Inf", "1e999 2 3",
	} {
		t.Run(input, func(t *testing.T) {
			got, err := parseLoadAverage([]byte(input))
			if err == nil {
				t.Fatal("accepted invalid input")
			}
			if got.Load1 != 0 || got.Load5 != 0 || got.Load15 != 0 {
				t.Fatalf("partial value: %+v", got)
			}
		})
	}
	for _, input := range []string{"x 2 3", "1e999 2 3"} {
		_, err := parseLoadAverage([]byte(input))
		var number *strconv.NumError
		if !errors.As(err, &number) {
			t.Fatalf("lost parse cause: %v", err)
		}
	}
}
