package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kefiriaus/otus-go-final-project/internal/config"
)

const (
	portFlag   = "-port"
	configFlag = "-config"
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
		{"defaults", nil, 0, "", ""},
		{"short help", []string{"-h"}, 0, portFlag, ""},
		{"long help", []string{"-help"}, 0, configFlag, ""},
		{"help skips file", []string{configFlag, missing, "-h"}, 0, "Usage", ""},
		{"unknown flag", []string{"-unknown"}, 2, "", "flag provided but not defined"},
		{"positional", []string{"extra"}, 2, "", "unexpected positional"},
		{"port type", []string{portFlag, "abc"}, 2, "", "invalid value"},
		{"port zero", []string{portFlag, "0"}, 2, "", "port"},
		{"port high", []string{portFlag, "65536"}, 2, "", "port"},
		{"empty config", []string{"-config="}, 2, "", "config path"},
		{"invalid file", []string{configFlag, invalid}, 2, "", invalid},
		{"missing file", []string{configFlag, missing}, 2, "", missing},
		{"valid file", []string{portFlag, "12345", configFlag, valid}, 0, "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runContext(
				context.Background(), tt.args, &stdout, &stderr,
				func(context.Context, config.Config) error { return nil },
			)
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
	got, err := parseArgs(
		[]string{configFlag, file, portFlag, "1000", portFlag, "12345"},
		io.Discard,
	)
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
		if _, err := parseArgs([]string{portFlag, port}, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{portFlag, "1", "-unknown"}, {portFlag, "1", "extra"}} {
		got, err := parseArgs(args, io.Discard)
		if err == nil || got != (config.Config{}) {
			t.Fatalf("got %+v %v for %v", got, err, args)
		}
	}
}
