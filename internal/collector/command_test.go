package collector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestCommandHelper(t *testing.T) {
	args := os.Args
	index := -1
	for i, a := range args {
		if a == "--collector-helper" {
			index = i + 1
			break
		}
	}
	if index < 0 {
		return
	}
	switch args[index] {
	case "success":
		fmt.Fprint(os.Stdout, "hello")
		fmt.Fprint(os.Stderr, "warning")
	case "exit":
		fmt.Fprint(os.Stderr, "failed")
		os.Exit(7)
	case "wait":
		time.Sleep(time.Hour)
	case "stdout":
		fmt.Fprint(os.Stdout, strings.Repeat("x", 65))
	case "stderr":
		fmt.Fprint(os.Stderr, strings.Repeat("x", 65))
	case "boundary":
		fmt.Fprint(os.Stdout, strings.Repeat("x", 64))
	}
	os.Exit(0)
}
func commandArgs(mode string) []string {
	return []string{"-test.run=^TestCommandHelper$", "--", "--collector-helper", mode}
}
func TestCommandRunnerSuccess(t *testing.T) {
	r := DefaultCommandRunner()
	r.Timeout = 3 * time.Second
	o, err := r.Run(context.Background(), os.Args[0], commandArgs("success")...)
	if err != nil || string(o.Stdout) != "hello" || string(o.Stderr) != "warning" {
		t.Fatalf("%+v %v", o, err)
	}
}
func TestCommandRunnerExitError(t *testing.T) {
	r := DefaultCommandRunner()
	r.Timeout = 3 * time.Second
	o, err := r.Run(context.Background(), os.Args[0], commandArgs("exit")...)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 7 || string(o.Stderr) != "failed" {
		t.Fatalf("%+v %v", o, err)
	}
}
func TestCommandRunnerStartError(t *testing.T) {
	if _, err := DefaultCommandRunner().Run(context.Background(), "/nonexistent/collector-command"); err == nil {
		t.Fatal("missing error")
	}
}
func TestCommandRunnerTimeout(t *testing.T) {
	r := DefaultCommandRunner()
	r.Timeout = 20 * time.Millisecond
	if _, err := r.Run(context.Background(), os.Args[0], commandArgs("wait")...); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
func TestCommandRunnerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(20*time.Millisecond, cancel)
	if _, err := DefaultCommandRunner().Run(ctx, os.Args[0], commandArgs("wait")...); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestCommandRunnerOutputLimit(t *testing.T) {
	r := CommandRunner{3 * time.Second, 64}
	for _, mode := range []string{"stdout", "stderr", "boundary"} {
		t.Run(mode, func(t *testing.T) {
			o, err := r.Run(context.Background(), os.Args[0], commandArgs(mode)...)
			if mode == "boundary" {
				if err != nil || len(o.Stdout) != 64 {
					t.Fatalf("%+v %v", o, err)
				}
				return
			}
			if !errors.Is(err, ErrOutputLimit) || len(o.Stdout) > 64 || len(o.Stderr) > 64 || len(o.Stdout)+len(o.Stderr) != 64 {
				t.Fatalf("%+v %v", o, err)
			}
		})
	}
}
func TestCommandRunnerValidation(t *testing.T) {
	for _, r := range []CommandRunner{{}, {time.Second, 0}, {0, 64}, {-1, 64}} {
		if _, err := r.Run(context.Background(), os.Args[0]); err == nil {
			t.Fatal("accepted options")
		}
	}
	if _, err := DefaultCommandRunner().Run(context.Background(), ""); err == nil {
		t.Fatal("accepted name")
	}
}
