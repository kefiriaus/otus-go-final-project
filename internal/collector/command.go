package collector

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

// CommandOutput preserves the bounded output even when a command fails.
type CommandOutput struct {
	Stdout []byte
	Stderr []byte
}

// CommandRunner executes short commands directly, without a shell.
// Cancellation kills the direct child, not its entire process tree.
type CommandRunner struct {
	Timeout        time.Duration
	MaxOutputBytes int
}

// ErrOutputLimit indicates that stdout or stderr was truncated.
var ErrOutputLimit = errors.New("command output limit exceeded")

// DefaultCommandRunner returns limits for short periodic commands.
func DefaultCommandRunner() CommandRunner { return CommandRunner{900 * time.Millisecond, 1 << 20} }

type limitedOutput struct {
	data     []byte
	limit    int
	exceeded bool
}

func (w *limitedOutput) Write(p []byte) (int, error) {
	n := len(p)
	available := w.limit - len(w.data)
	if len(p) > available {
		p = p[:available]
		w.exceeded = true
	}
	w.data = append(w.data, p...)
	return n, nil
}

// Run waits for the command and bounds output retained from each stream.
func (r CommandRunner) Run(ctx context.Context, name string, args ...string) (CommandOutput, error) {
	if r.Timeout <= 0 || r.MaxOutputBytes <= 0 || name == "" {
		return CommandOutput{}, fmt.Errorf("command requires a name, positive timeout and output limit")
	}
	attempt, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	cmd := exec.CommandContext(attempt, name, args...)
	cmd.WaitDelay = 100 * time.Millisecond
	stdout := limitedOutput{limit: r.MaxOutputBytes}
	stderr := limitedOutput{limit: r.MaxOutputBytes}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if cause := attempt.Err(); cause != nil {
		err = errors.Join(err, cause)
	}
	if stdout.exceeded || stderr.exceeded {
		err = errors.Join(err, ErrOutputLimit)
	}
	if err != nil {
		err = fmt.Errorf("command %q: %w", name, err)
	}
	return CommandOutput{Stdout: stdout.data, Stderr: stderr.data}, err
}
