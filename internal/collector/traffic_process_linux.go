package collector

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

var errTrafficSessionStopped = errors.New("capture stopped before header readiness")

// trafficAvailability serializes activation against every failure/stop path.
// Once stopped, a late header cannot reactivate an already failed session.
type trafficAvailability struct {
	mu      sync.Mutex
	sink    TrafficSink
	name    string
	start   time.Time
	stopped bool
	err     error
}

func (a *trafficAvailability) activate(at time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stopped {
		return errTrafficSessionStopped
	}
	a.start = at
	return a.sink.SetInterfaceState(a.name, at, true)
}

func (a *trafficAvailability) stop(at time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stopped {
		return a.err
	}
	a.stopped = true
	if at.Before(a.start) {
		at = a.start
	}
	a.err = a.sink.SetInterfaceState(a.name, at, false)
	return a.err
}

type trafficDiagnostics struct {
	mu               sync.Mutex
	tail             [4096]byte
	size             int
	drops            uint64
	report           time.Time
	pendingRequest   time.Time
	sampleLowerBound time.Time
	start            time.Time
}

func (d *trafficDiagnostics) append(line []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(line) >= len(d.tail) {
		copy(d.tail[:], line[len(line)-len(d.tail):])
		d.size = len(d.tail)
		return
	}
	if d.size+len(line) > len(d.tail) {
		remove := d.size + len(line) - len(d.tail)
		copy(d.tail[:], d.tail[remove:d.size])
		d.size -= remove
	}
	copy(d.tail[d.size:], line)
	d.size += len(line)
}

func (d *trafficDiagnostics) text() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return string(d.tail[:d.size])
}

func (d *trafficDiagnostics) lastReport() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.report
}

func (d *trafficDiagnostics) dropReport(line string, now time.Time, sink TrafficSink) error {
	if !strings.Contains(line, "dropped by kernel") {
		return nil
	}
	fields := strings.Fields(line)
	var n uint64
	var err error
	malformed := len(fields) != 5 ||
		(fields[1] != "packets" && fields[1] != "packet") ||
		strings.Join(fields[2:], " ") != "dropped by kernel"
	switch {
	case malformed:
		err = fmt.Errorf("malformed tcpdump drop telemetry")
	case len(fields[0]) > 20:
		err = fmt.Errorf("tcpdump drop count exceeds uint64 length")
	default:
		n, err = strconv.ParseUint(fields[0], 10, 64)
	}
	d.mu.Lock()
	previous, start := d.drops, d.sampleLowerBound
	if start.IsZero() {
		start = d.start
	}
	d.report = now
	if !d.pendingRequest.IsZero() {
		d.sampleLowerBound = d.pendingRequest
		d.pendingRequest = time.Time{}
	} else if d.sampleLowerBound.IsZero() {
		d.sampleLowerBound = d.start
	}
	d.drops = n
	d.mu.Unlock()
	if err != nil || n != previous {
		if !start.Before(now) {
			start = now.Add(-time.Nanosecond)
		}
		if failure := sink.MarkLoss(start, now); failure != nil {
			return sinkResult(failure)
		}
	}
	if err != nil {
		return fmt.Errorf("tcpdump drop telemetry: %w", err)
	}
	return nil
}

func (c *TrafficCapture) processCapturedPacket(
	frame pcapFrame,
	name string,
	started time.Time,
	sink TrafficSink,
) error {
	packet, ip, err := decodeTrafficFrame(frame, name)
	if err != nil {
		c.setError(err)
		return c.packetLoss(sink, frame.At)
	}
	if !ip {
		return nil
	}
	if packet.At.Before(started) {
		packet.Partial = true
		if err := sink.MarkLoss(packet.At, started); err != nil {
			return sinkResult(err)
		}
	}
	if err := sink.RecordPacket(packet); err != nil {
		if !errors.Is(err, metrics.ErrTrafficPacketRejected) {
			return sinkResult(err)
		}
		c.setError(err)
		if failure := c.packetLoss(sink, packet.At); failure != nil {
			return sinkResult(failure)
		}
	}
	return nil
}

func (c *TrafficCapture) readCaptureOutput(
	stdout *os.File,
	availability *trafficAvailability,
	diagnostic *trafficDiagnostics,
	ready chan<- time.Time,
	outputDone chan<- error,
	name string,
) {
	defer func() { _ = availability.stop(time.Now().UTC()) }()
	reader, err := newPCAPReader(stdout, maxTrafficFrameBytes)
	if err != nil {
		outputDone <- err
		return
	}
	started := time.Now().UTC()
	if err := availability.activate(started); err != nil {
		if errors.Is(err, errTrafficSessionStopped) {
			outputDone <- err
			return
		}
		outputDone <- sinkResult(err)
		return
	}
	ready <- started
	for {
		frame, err := reader.Next()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				if failure := availability.sink.MarkLoss(
					diagnostic.start, time.Now().UTC().Add(time.Nanosecond),
				); failure != nil {
					err = errors.Join(err, sinkResult(failure))
				}
			}
			outputDone <- err
			return
		}
		if err := c.processCapturedPacket(frame, name, started, availability.sink); err != nil {
			outputDone <- err
			return
		}
	}
}

func (c *TrafficCapture) readCaptureDiagnostics(
	stderr *os.File,
	availability *trafficAvailability,
	diagnostic *trafficDiagnostics,
	stderrDone chan<- error,
) {
	defer func() { _ = availability.stop(time.Now().UTC()) }()
	scanner := bufio.NewScanner(stderr)
	scanner.Buffer(make([]byte, 4096), 65536)
	for scanner.Scan() {
		line := scanner.Text()
		diagnostic.append(append([]byte(line), '\n'))
		if err := diagnostic.dropReport(line, time.Now().UTC(), availability.sink); err != nil {
			var fatal *trafficSinkError
			if errors.As(err, &fatal) {
				stderrDone <- err
				return
			}
			c.setError(err)
		}
	}
	stderrDone <- scanner.Err()
}

func (c *TrafficCapture) requestDropTelemetry(
	cmd *exec.Cmd,
	sink TrafficSink,
	diagnostic *trafficDiagnostics,
	name string,
	started bool,
	readyAt time.Time,
	pending time.Time,
) (time.Time, error) {
	if !started {
		return pending, nil
	}
	now := time.Now().UTC()
	if now.Before(readyAt.Add(c.interval)) {
		return pending, nil
	}
	if !pending.IsZero() && diagnostic.lastReport().Before(pending) {
		c.setError(fmt.Errorf("missing tcpdump drop telemetry on %s", name))
		if err := sink.MarkLoss(pending, now); err != nil {
			return pending, sinkResult(err)
		}
	}
	diagnostic.noteRequest(now)
	if err := cmd.Process.Signal(syscall.SIGUSR1); err != nil {
		return pending, err
	}
	return now, nil
}

func (c *TrafficCapture) runSessionEvents(
	ctx context.Context,
	cmd *exec.Cmd,
	sink TrafficSink,
	name string,
	diagnostic *trafficDiagnostics,
	ready <-chan time.Time,
	outputDone <-chan error,
	stderrDone <-chan error,
	waited <-chan error,
) (result error, processExited bool, waitError error) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	started := false
	var readyAt time.Time
	var pending time.Time
	for {
		select {
		case <-ctx.Done():
			return ctx.Err(), processExited, waitError
		case readyAt = <-ready:
			started = true
		case err := <-outputDone:
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return err, processExited, waitError
		case err := <-stderrDone:
			if err == nil {
				err = fmt.Errorf("tcpdump stderr closed")
			}
			return err, processExited, waitError
		case err := <-waited:
			processExited = true
			waitError = err
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return err, processExited, waitError
		case <-ticker.C:
			var err error
			pending, err = c.requestDropTelemetry(
				cmd, sink, diagnostic, name, started, readyAt, pending,
			)
			if err != nil {
				return err, processExited, waitError
			}
		}
	}
}

func (c *TrafficCapture) stopCaptureProcess(
	cmd *exec.Cmd,
	waited <-chan error,
	processExited bool,
	waitError error,
	sink TrafficSink,
	start time.Time,
	result error,
) (error, error) {
	if processExited {
		return waitError, result
	}
	_ = cmd.Process.Signal(syscall.SIGINT)
	timer := time.NewTimer(c.grace)
	select {
	case waitError = <-waited:
		timer.Stop()
	case <-timer.C:
		if failure := sink.MarkLoss(start, time.Now().UTC().Add(time.Nanosecond)); failure != nil {
			result = errors.Join(result, sinkResult(failure))
		}
		_ = cmd.Process.Kill()
		waitError = <-waited
	}
	return waitError, result
}

func (c *TrafficCapture) joinCaptureReaders(
	readers *sync.WaitGroup,
	stdout, stderr *os.File,
	sink TrafficSink,
	start time.Time,
	result error,
) error {
	joined := make(chan struct{})
	go func() { readers.Wait(); close(joined) }()
	timer := time.NewTimer(c.grace)
	select {
	case <-joined:
		timer.Stop()
	case <-timer.C:
		if failure := sink.MarkLoss(start, time.Now().UTC().Add(time.Nanosecond)); failure != nil {
			result = errors.Join(result, sinkResult(failure))
		}
		_ = stdout.Close()
		_ = stderr.Close()
		<-joined
	}
	return result
}

func drainCaptureResults(result error, outputDone, stderrDone <-chan error) error {
	select {
	case err := <-outputDone:
		if err != nil {
			result = errors.Join(result, err)
		}
	default:
	}
	select {
	case err := <-stderrDone:
		if err != nil {
			result = errors.Join(result, err)
		}
	default:
	}
	return result
}

func (c *TrafficCapture) session(
	ctx context.Context,
	sink TrafficSink,
	name string,
) (result error) {
	cmd := c.command(name)
	stdout, stdoutWrite, err := os.Pipe()
	if err != nil {
		return err
	}
	defer stdout.Close()
	defer stdoutWrite.Close()
	stderr, stderrWrite, err := os.Pipe()
	if err != nil {
		return err
	}
	defer stderr.Close()
	defer stderrWrite.Close()
	cmd.Stdout = stdoutWrite
	cmd.Stderr = stderrWrite
	cmd.WaitDelay = 100 * time.Millisecond
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start tcpdump on %s: %w", name, err)
	}
	_ = stdoutWrite.Close()
	_ = stderrWrite.Close()
	availability := &trafficAvailability{sink: sink, name: name}
	waited := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		if failure := availability.stop(time.Now().UTC()); failure != nil {
			err = errors.Join(err, sinkResult(failure))
		}
		waited <- err
	}()
	ready := make(chan time.Time, 1)
	outputDone := make(chan error, 1)
	stderrDone := make(chan error, 1)
	var readers sync.WaitGroup
	readers.Add(2)
	diagnostic := &trafficDiagnostics{start: time.Now().UTC()}
	go func() {
		defer readers.Done()
		c.readCaptureOutput(stdout, availability, diagnostic, ready, outputDone, name)
	}()
	go func() {
		defer readers.Done()
		c.readCaptureDiagnostics(stderr, availability, diagnostic, stderrDone)
	}()
	result, processExited, waitError := c.runSessionEvents(
		ctx, cmd, sink, name, diagnostic, ready, outputDone, stderrDone, waited,
	)
	if err := availability.stop(time.Now().UTC()); err != nil {
		result = errors.Join(result, sinkResult(err))
	}
	if ctx.Err() == nil {
		// A failed session cannot promise completeness before its failure was detected.
		if err := sink.MarkLoss(
			diagnostic.start,
			time.Now().UTC().Add(time.Nanosecond),
		); err != nil {
			result = errors.Join(result, sinkResult(err))
		}
	}
	// Own pipe descriptors avoid Cmd.Wait closing stdout before it is drained.
	waitError, result = c.stopCaptureProcess(
		cmd, waited, processExited, waitError, sink, diagnostic.start, result,
	)
	result = c.joinCaptureReaders(&readers, stdout, stderr, sink, diagnostic.start, result)
	result = errors.Join(result, waitError)
	result = drainCaptureResults(result, outputDone, stderrDone)
	if err := availability.stop(time.Now().UTC()); err != nil {
		return sinkResult(err)
	}
	var fatal *trafficSinkError
	if errors.As(result, &fatal) {
		return result
	}
	return fmt.Errorf("tcpdump on %s: %w; stderr: %s", name, result, diagnostic.text())
}

func (c *TrafficCapture) packetLoss(sink TrafficSink, at time.Time) error {
	now := time.Now().UTC()
	if at.IsZero() || at.After(now) {
		at = now.Add(-c.interval)
	}
	return sink.MarkLoss(at, now.Add(time.Nanosecond))
}

// noteRequest records a lower bound on when the next kernel counters are sampled.
func (d *trafficDiagnostics) noteRequest(at time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pendingRequest.IsZero() {
		d.pendingRequest = at
	}
}
