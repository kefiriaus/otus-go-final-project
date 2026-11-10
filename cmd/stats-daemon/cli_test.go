package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kefiriaus/otus-go-final-project/internal/config"
)

func configFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestRun(t *testing.T) {
	valid := configFile(t, `{"subsystems":{"cpu":false}}`)
	invalid := configFile(t, `{`)
	missing := filepath.Join(t.TempDir(), "missing.json")
	for _, tt := range []struct {
		name     string
		args     []string
		code     int
		out, err string
	}{
		{"defaults", nil, 1, "", "scaffold only"},
		{"short help", []string{"-h"}, 0, "-port", ""},
		{"long help", []string{"-help"}, 0, "-config", ""},
		{"help skips file", []string{"-config", missing, "-h"}, 0, "Usage", ""},
		{"unknown flag", []string{"-unknown"}, 2, "", "flag provided but not defined"},
		{"positional", []string{"extra"}, 2, "", "unexpected positional"},
		{"port type", []string{"-port", "abc"}, 2, "", "invalid value"},
		{"port zero", []string{"-port", "0"}, 2, "", "port"},
		{"port high", []string{"-port", "65536"}, 2, "", "port"},
		{"empty config", []string{"-config="}, 2, "", "config path"},
		{"invalid file", []string{"-config", invalid}, 2, "", invalid},
		{"missing file", []string{"-config", missing}, 2, "", missing},
		{"valid file", []string{"-port", "12345", "-config", valid}, 1, "", "scaffold only"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr)
			if code != tt.code {
				t.Fatalf("exit=%d want=%d stderr=%q", code, tt.code, stderr.String())
			}
			if tt.out == "" {
				if stdout.Len() != 0 {
					t.Fatalf("unexpected stdout %q", stdout.String())
				}
			} else if !strings.Contains(stdout.String(), tt.out) {
				t.Fatalf("stdout %q missing %q", stdout.String(), tt.out)
			}
			if tt.err == "" {
				if stderr.Len() != 0 {
					t.Fatalf("unexpected stderr %q", stderr.String())
				}
			} else if !strings.Contains(stderr.String(), tt.err) {
				t.Fatalf("stderr %q missing %q", stderr.String(), tt.err)
			}
			if tt.code == 2 && strings.Count(stderr.String(), "stats-daemon:") != 1 {
				t.Fatalf("error not printed exactly once: %q", stderr.String())
			}
		})
	}
}
func TestParseArgsConfig(t *testing.T) {
	file := configFile(t, `{"max_window_seconds":60,"subsystems":{"cpu":false}}`)
	got, err := parseArgs([]string{"-config", file, "-port", "1000", "-port", "12345"}, io.Discard)
	want := config.Default()
	want.Port = 12345
	want.MaxWindowSeconds = 60
	want.Subsystems.CPU = false
	if err != nil || got != want {
		t.Fatalf("got %+v, %v; want %+v", got, err, want)
	}
	got, err = parseArgs(nil, io.Discard)
	if err != nil || got != config.Default() {
		t.Fatalf("state leaked: %+v %v", got, err)
	}
	for _, port := range []string{"1", "65535"} {
		if _, err := parseArgs([]string{"-port", port}, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"-port", "1", "-unknown"}, {"-port", "1", "extra"}} {
		got, err := parseArgs(args, io.Discard)
		if err == nil || got != (config.Config{}) {
			t.Fatalf("got %+v %v for %v", got, err, args)
		}
	}
}
