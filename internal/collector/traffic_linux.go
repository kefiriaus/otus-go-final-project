package collector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"sync"
	"time"
)

// TrafficCaptureOptions selects a fixed startup set. Limits are 1..8 interfaces
// and a retry delay in [1 second, 1 minute]. Loopback requires explicit opt-in.
type TrafficCaptureOptions struct {
	Interfaces      []string
	IncludeLoopback bool
	MaxInterfaces   int
	RetryInterval   time.Duration
}

// DefaultTrafficCaptureOptions selects up, non-loopback interfaces.
func DefaultTrafficCaptureOptions() TrafficCaptureOptions {
	return TrafficCaptureOptions{MaxInterfaces: 8, RetryInterval: time.Second}
}

func selectTrafficInterfaces(all []net.Interface, o TrafficCaptureOptions) ([]string, error) {
	if o.MaxInterfaces < 1 || o.MaxInterfaces > 8 || o.RetryInterval < time.Second ||
		o.RetryInterval > time.Minute {
		return nil, fmt.Errorf("invalid traffic capture limits")
	}
	byName := make(map[string]net.Interface)
	for _, iface := range all {
		byName[iface.Name] = iface
	}
	requested := slices.Clone(o.Interfaces)
	if len(requested) == 0 {
		for _, iface := range all {
			if iface.Flags&net.FlagUp != 0 &&
				(o.IncludeLoopback || iface.Flags&net.FlagLoopback == 0) {
				requested = append(requested, iface.Name)
			}
		}
	}
	if len(requested) == 0 || len(requested) > o.MaxInterfaces {
		return nil, fmt.Errorf(
			"traffic capture requires 1..%d eligible interfaces",
			o.MaxInterfaces,
		)
	}
	slices.Sort(requested)
	for i, name := range requested {
		iface, ok := byName[name]
		if !ok || iface.Flags&net.FlagUp == 0 ||
			(!o.IncludeLoopback && iface.Flags&net.FlagLoopback != 0) ||
			(i > 0 && requested[i-1] == name) {
			return nil, fmt.Errorf("invalid, duplicate, down, or excluded interface %q", name)
		}
	}
	return requested, nil
}

// TrafficCapture supervises independent continuous Linux tcpdump sessions.
// Construct it with NewTrafficCapture; only one Run may be active at a time.
type TrafficCapture struct {
	names           []string
	options         TrafficCaptureOptions
	command         func(string) *exec.Cmd
	interval, grace time.Duration
	mu              sync.Mutex
	lastError       error
	running         bool
}

// NewTrafficCapture resolves interfaces without opening a packet capture.
func NewTrafficCapture(o TrafficCaptureOptions) (*TrafficCapture, error) {
	all, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("list capture interfaces: %w", err)
	}
	names, err := selectTrafficInterfaces(all, o)
	if err != nil {
		return nil, err
	}
	c := &TrafficCapture{
		names:    names,
		options:  o,
		interval: time.Second,
		grace:    500 * time.Millisecond,
	}
	c.command = func(name string) *exec.Cmd {
		// #nosec G204,G702 -- Interface names are validated during construction and no shell is used.
		cmd := exec.CommandContext(context.Background(),
			"tcpdump",
			"-n",
			"-p",
			"-U",
			"-Q",
			"inout",
			"-s",
			strconv.Itoa(maxTrafficFrameBytes),
			"-i",
			name,
			"-w",
			"-",
		)
		cmd.Env = append(os.Environ(), "LC_ALL=C")
		return cmd
	}
	return c, nil
}

// Interfaces returns a copy of the fixed capture selection.
func (c *TrafficCapture) Interfaces() []string { return slices.Clone(c.names) }

// LastError returns the most recent bounded capture diagnostic.
func (c *TrafficCapture) LastError() error   { c.mu.Lock(); defer c.mu.Unlock(); return c.lastError }
func (c *TrafficCapture) setError(err error) { c.mu.Lock(); defer c.mu.Unlock(); c.lastError = err }

type trafficSinkError struct{ error }

func (e *trafficSinkError) Unwrap() error { return e.error }

func sinkResult(err error) error {
	if err == nil {
		return nil
	}
	return &trafficSinkError{err}
}

func (c *TrafficCapture) runSessionWithRetry(
	group context.Context,
	cancel context.CancelFunc,
	sink TrafficSink,
	name string,
	failures chan<- error,
) {
	for group.Err() == nil {
		err := c.session(group, sink, name)
		if group.Err() != nil {
			return
		}
		var fatal *trafficSinkError
		if errors.As(err, &fatal) {
			select {
			case failures <- fatal.error:
			default:
			}
			cancel()
			return
		}
		c.setError(err)
		retry := time.NewTimer(c.options.RetryInterval)
		select {
		case <-retry.C:
		case <-group.Done():
			retry.Stop()
			return
		}
	}
}

// Run retries capture failures internally; they are not terminal RunGroup errors.
// Cancellation stops and joins all owned goroutines and direct child processes.
func (c *TrafficCapture) Run(ctx context.Context, sink TrafficSink) error {
	if sink == nil {
		return fmt.Errorf("traffic sink must not be nil")
	}
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("traffic capture already running")
	}
	c.running = true
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.running = false; c.mu.Unlock() }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := sink.Advance(time.Now().UTC()); err != nil {
		return err
	}
	group, cancel := context.WithCancel(ctx)
	defer cancel()
	failures := make(chan error, 1)
	var workers sync.WaitGroup
	for _, name := range c.names {
		workers.Add(1)
		go func() {
			defer workers.Done()
			c.runSessionWithRetry(group, cancel, sink, name, failures)
		}()
	}
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	var result error
loop:
	for {
		select {
		case <-ctx.Done():
			result = ctx.Err()
			break loop
		case err := <-failures:
			result = err
			break loop
		case <-group.Done():
			select {
			case result = <-failures:
			default:
				result = group.Err()
			}
			break loop
		case <-ticker.C:
			if err := sink.Advance(time.Now().UTC()); err != nil {
				result = err
				break loop
			}
		}
	}
	cancel()
	workers.Wait()
	return result
}
