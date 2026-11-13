package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestParseArgs(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want clientOptions
	}{
		{"defaults", nil, clientOptions{address: "127.0.0.1:50051", intervalSeconds: 5, windowSeconds: 15}},
		{"custom", []string{"-address", "localhost:6000", "-interval", "2", "-window", "30"}, clientOptions{address: "localhost:6000", intervalSeconds: 2, windowSeconds: 30}},
		{"minimum", []string{"-interval", "1", "-window", "1"}, clientOptions{address: "127.0.0.1:50051", intervalSeconds: 1, windowSeconds: 1}},
		{"maximum", []string{"-interval", "3600", "-window", "3600"}, clientOptions{address: "127.0.0.1:50051", intervalSeconds: 3600, windowSeconds: 3600}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArgs(tt.args, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
	for _, tt := range []struct {
		name string
		args []string
	}{
		{"empty address", []string{"-address="}},
		{"positional", []string{"extra"}},
		{"unknown flag", []string{"-unknown"}},
		{"interval type", []string{"-interval", "abc"}},
		{"window type", []string{"-window", "abc"}},
		{"interval zero", []string{"-interval", "0"}},
		{"interval high", []string{"-interval", "3601"}},
		{"window zero", []string{"-window", "0"}},
		{"window high", []string{"-window", "3601"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArgs(tt.args, io.Discard)
			if err == nil {
				t.Fatal("expected error")
			}
			if got != (clientOptions{}) {
				t.Fatalf("got %+v on error", got)
			}
		})
	}
}

func TestRunContextExitCodes(t *testing.T) {
	for _, tt := range []struct {
		name       string
		receiveErr error
		wantCode   int
		wantStderr string
	}{
		{"success", nil, 0, ""},
		{"cancellation", context.Canceled, 0, ""},
		{"stream failure", errors.New("stream failed"), 1, "stats-client: stream failed\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runContext(context.Background(), nil, &stdout, &stderr, func(context.Context, clientOptions, io.Writer) error {
				return tt.receiveErr
			})
			if code != tt.wantCode || stderr.String() != tt.wantStderr {
				t.Fatalf("code=%d stderr=%q, want code=%d stderr=%q", code, stderr.String(), tt.wantCode, tt.wantStderr)
			}
		})
	}
}

func TestRunContextHelp(t *testing.T) {
	for _, arg := range []string{"-h", "-help"} {
		t.Run(arg, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			called := false
			code := runContext(context.Background(), []string{arg}, &stdout, &stderr, func(context.Context, clientOptions, io.Writer) error {
				called = true
				return nil
			})
			if code != 0 || called || stderr.Len() != 0 {
				t.Fatalf("code=%d called=%v stderr=%q", code, called, stderr.String())
			}
			for _, flag := range []string{"-address", "-interval", "-window"} {
				if !strings.Contains(stdout.String(), flag) {
					t.Fatalf("usage %q missing %s", stdout.String(), flag)
				}
			}
		})
	}
}
