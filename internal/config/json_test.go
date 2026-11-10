package config

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestLoadJSONDefaultsAndOverrides(t *testing.T) {
	cpuOff := Default()
	cpuOff.Subsystems.CPU = false
	short := Default()
	short.MaxWindowSeconds = 60
	allOff := Default()
	allOff.Subsystems = Subsystems{}
	for _, tt := range []struct {
		name, input string
		want        Config
	}{
		{"defaults", `{}`, Default()},
		{"CPU disabled", `{"subsystems":{"cpu":false}}`, cpuOff},
		{"short window", `{"max_window_seconds":60}`, short},
		{"all disabled", `{"subsystems":{"load_average":false,"cpu":false,"disk_io":false,"filesystems":false,"traffic":false,"connections":false}}`, allOff},
		{"duplicate field", `{"subsystems":{"cpu":true,"cpu":false}}`, cpuOff},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadJSON(strings.NewReader(tt.input))
			if err != nil || got != tt.want {
				t.Fatalf("got %+v, %v; want %+v", got, err, tt.want)
			}
		})
	}
}
func TestLoadJSONRejectsInvalidInput(t *testing.T) {
	for _, input := range []string{
		``, `null`, `[]`, `{"subsystems":null}`, `{"subsystems":{"cpu":null}}`, `{"max_window_seconds":null}`,
		`{"unknown":true}`, `{"subsystems":{"unknown":true}}`, `{"port":50051}`,
		`{"subsystems":{"cpu":"false"}}`, `{"subsystems":{"cpu":0}}`, `{"max_window_seconds":"60"}`, `{"max_window_seconds":1.5}`,
		`{"max_window_seconds":0}`, `{"max_window_seconds":3601}`, `{`, `{} {}`, `{} garbage`,
	} {
		t.Run(input, func(t *testing.T) {
			got, err := LoadJSON(strings.NewReader(input))
			if err == nil || got != (Config{}) {
				t.Fatalf("got %+v, %v; want zero config and error", got, err)
			}
		})
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }
func TestLoadJSONReaderError(t *testing.T) {
	want := errors.New("read interrupted")
	got, err := LoadJSON(errorReader{want})
	if !errors.Is(err, want) || got != (Config{}) {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestExampleConfig(t *testing.T) {
	file, err := os.Open("../../configs/stats-daemon.json")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	got, err := LoadJSON(file)
	if err != nil || got != Default() {
		t.Fatalf("example: %+v, %v", got, err)
	}
}
