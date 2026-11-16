package collector

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const (
	testCaptureInterface = "eth0"
	testPacketMode       = "packet"
	testMissingStatsMode = "missing stats"
	testDropsMode        = "drops"
	testMissingMode      = "missing"
	testBadInterface     = "bad"
)

func handleTrafficHelperSignal(mode string, signalValue os.Signal) bool {
	switch signalValue {
	case syscall.SIGUSR1:
		if mode == "framing" {
			record := pcapRecord(binary.LittleEndian, 1000, 0, maxTrafficFrameBytes+1, nil)
			binary.LittleEndian.PutUint32(record[8:], maxTrafficFrameBytes+1)
			_, _ = os.Stdout.Write(record)
			return false
		}
		if mode != testMissingStatsMode {
			drops := 0
			if mode == testDropsMode {
				drops = 2
			}
			fmt.Fprintf(os.Stderr, "1 packet captured\n1 packet received by filter\n%d packets dropped by kernel\n", drops)
		}
	case syscall.SIGINT:
		if mode == "flush" {
			now := time.Now().Add(-time.Millisecond)
			// #nosec G115 -- Current Unix seconds and nanoseconds fit the PCAP uint32 fields.
			_, _ = os.Stdout.Write(pcapRecord(
				binary.LittleEndian, uint32(now.Unix()), uint32(now.Nanosecond()),
				28, ipv4Packet(17, 321, 654),
			))
		}
		return true
	}
	return false
}

func TestTrafficHelper(_ *testing.T) {
	args := os.Args
	index := -1
	for i, arg := range args {
		if arg == "traffic-helper" {
			index = i + 1
			break
		}
	}
	if index < 0 {
		return
	}
	mode := args[index]
	if mode == "permission" {
		fmt.Fprintln(os.Stderr, "permission denied")
		os.Exit(7)
	}
	signals := make(chan os.Signal, 8)
	signal.Notify(signals, syscall.SIGUSR1, syscall.SIGINT)
	defer signal.Stop(signals)
	if mode == "ignore" || mode == "framing" {
		signal.Ignore(syscall.SIGINT)
	}
	if mode == "malformed" {
		_, _ = os.Stdout.Write([]byte("not pcap"))
		//nolint:gocritic // Explicit exit keeps test-framework output out of the fake PCAP stream.
		os.Exit(0)
	}
	header := pcapHeader(binary.LittleEndian, true, 101, maxTrafficFrameBytes)
	for _, piece := range [][]byte{header[:5], header[5:]} {
		_, _ = os.Stdout.Write(piece)
	}
	if mode == testPacketMode || mode == "delayed malformed" {
		now := time.Now()
		data := ipv4Packet(17, 123, 456)
		if mode == "delayed malformed" {
			now = now.Add(-3 * time.Second)
			data[0] = 0x75
		}
		// #nosec G115 -- Current Unix seconds and nanoseconds fit the PCAP uint32 fields.
		_, _ = os.Stdout.Write(
			pcapRecord(binary.LittleEndian, uint32(now.Unix()), uint32(now.Nanosecond()), 28, data),
		)
	}
	if mode == "flood" {
		fmt.Fprintln(os.Stderr, strings.Repeat("x", 70000))
	}
	if mode == "eof" {
		os.Exit(0)
	}
	for {
		if handleTrafficHelperSignal(mode, <-signals) {
			os.Exit(0)
		}
	}
}

type captureTestSink struct {
	lossStart time.Time
	mu        sync.Mutex
	active    map[string]bool
	losses    int
	ready     chan struct{}
	once      sync.Once
	packets   chan metrics.TrafficPacket
	failure   error
}

func newCaptureTestSink() *captureTestSink {
	return &captureTestSink{
		active:  make(map[string]bool),
		ready:   make(chan struct{}),
		packets: make(chan metrics.TrafficPacket, 32),
	}
}

func (s *captureTestSink) SetInterfaceState(name string, _ time.Time, active bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[name] = active
	if active {
		s.once.Do(func() { close(s.ready) })
	}
	return s.failure
}

func (s *captureTestSink) RecordPacket(p metrics.TrafficPacket) error {
	select {
	case s.packets <- p:
	default:
	}
	return s.failure
}

func (s *captureTestSink) MarkLoss(start, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.losses++
	if s.lossStart.IsZero() || start.Before(s.lossStart) {
		s.lossStart = start
	}
	return s.failure
}
func (s *captureTestSink) Advance(time.Time) error { return s.failure }
func testTrafficCapture(mode string) *TrafficCapture {
	c := &TrafficCapture{
		names:    []string{testCaptureInterface},
		options:  DefaultTrafficCaptureOptions(),
		interval: 10 * time.Millisecond,
		grace:    10 * time.Millisecond,
	}
	c.options.RetryInterval = 5 * time.Millisecond
	c.command = func(string) *exec.Cmd {
		// #nosec G204,G702 -- The helper executes this test's own binary with a fixed argument list.
		return exec.CommandContext(
			context.Background(),
			os.Args[0],
			"-test.run=^TestTrafficHelper$",
			"--",
			"traffic-helper",
			mode,
		)
	}
	return c
}

func TestTrafficSelection(t *testing.T) {
	all := []net.Interface{
		{Name: "lo", Flags: net.FlagUp | net.FlagLoopback},
		{Name: "eth1", Flags: net.FlagUp},
		{Name: testCaptureInterface, Flags: net.FlagUp},
		{Name: "down"},
	}
	defaults := DefaultTrafficCaptureOptions()
	names, err := selectTrafficInterfaces(all, defaults)
	if err != nil || strings.Join(names, ",") != "eth0,eth1" {
		t.Fatalf("%v %v", names, err)
	}
	for _, requested := range [][]string{
		{"lo"}, {testMissingMode}, {"down"}, {testCaptureInterface, testCaptureInterface},
	} {
		o := defaults
		o.Interfaces = requested
		if _, err := selectTrafficInterfaces(all, o); err == nil {
			t.Fatal(requested)
		}
	}
	o := defaults
	o.Interfaces = []string{"lo"}
	o.IncludeLoopback = true
	if _, err := selectTrafficInterfaces(all, o); err != nil {
		t.Fatal(err)
	}
	o = defaults
	o.MaxInterfaces = 1
	if _, err := selectTrafficInterfaces(all, o); err == nil {
		t.Fatal("selection truncated")
	}
	o = defaults
	o.RetryInterval = 0
	if _, err := selectTrafficInterfaces(all, o); err == nil {
		t.Fatal("invalid options")
	}
	if _, err := selectTrafficInterfaces(nil, defaults); err == nil {
		t.Fatal("empty selection")
	}
}

func waitForCapturePacket(t *testing.T, sink *captureTestSink, cancel context.CancelFunc) {
	t.Helper()
	select {
	case p := <-sink.packets:
		if p.IPBytes != 28 || p.Flow.Source.Port != 123 {
			t.Fatal(p)
		}
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("packet missing")
	}
}

func waitForCaptureLoss(t *testing.T, sink *captureTestSink, cancel context.CancelFunc) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		sink.mu.Lock()
		losses := sink.losses
		sink.mu.Unlock()
		if losses > 0 {
			return
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("loss not reported")
		}
		time.Sleep(time.Millisecond)
	}
}

func stopCaptureTest(t *testing.T, cancel context.CancelFunc, done <-chan error, sink *captureTestSink) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("capture cleanup blocked")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, active := range sink.active {
		if active {
			t.Fatal("left active")
		}
	}
}

func TestTrafficCaptureLifecycle(t *testing.T) {
	for _, mode := range []string{"quiet", testPacketMode, "ignore", testDropsMode, testMissingStatsMode} {
		t.Run(mode, func(t *testing.T) {
			c := testTrafficCapture(mode)
			sink := newCaptureTestSink()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- c.Run(ctx, sink) }()
			select {
			case <-sink.ready:
			case <-time.After(3 * time.Second):
				cancel()
				t.Fatal("capture did not start")
			}
			if mode == testPacketMode {
				waitForCapturePacket(t, sink, cancel)
			}
			if mode == testDropsMode || mode == testMissingStatsMode {
				waitForCaptureLoss(t, sink, cancel)
			}
			stopCaptureTest(t, cancel, done, sink)
			if mode == "quiet" {
				sink.mu.Lock()
				defer sink.mu.Unlock()
				if sink.losses != 0 {
					t.Fatalf("quiet cancellation added %d loss records", sink.losses)
				}
			}
		})
	}
}

func TestDropTelemetryUsesSignalTime(t *testing.T) {
	c := testTrafficCapture("quiet")
	cmd := c.command(testCaptureInterface)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	diagnostic := &trafficDiagnostics{start: time.Now().UTC()}
	sink := newCaptureTestSink()
	before := time.Now().UTC()
	pending, err := c.requestDropTelemetry(
		cmd, sink, diagnostic, testCaptureInterface, true, time.Time{}, time.Time{},
	)
	after := time.Now().UTC()
	if err != nil {
		t.Fatal(err)
	}
	if pending.Before(before) || pending.After(after) ||
		!pending.Equal(diagnostic.pendingRequest) {
		t.Fatalf("request time %v, diagnostic %v, expected within [%v,%v]", pending, diagnostic.pendingRequest, before, after)
	}
}

func TestTrafficCaptureFailureAndRetry(t *testing.T) {
	for _, mode := range []string{"permission", "malformed", "eof", "flood", "missing binary"} {
		t.Run(mode, func(t *testing.T) {
			c := testTrafficCapture(mode)
			original := c.command
			var launches atomic.Int32
			c.command = func(name string) *exec.Cmd {
				launches.Add(1)
				if mode == "missing binary" {
					return exec.CommandContext(context.Background(), "/nonexistent/tcpdump")
				}
				return original(name)
			}
			sink := newCaptureTestSink()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- c.Run(ctx, sink) }()
			deadline := time.Now().Add(3 * time.Second)
			for launches.Load() < 2 || c.LastError() == nil {
				if time.Now().After(deadline) {
					cancel()
					t.Fatalf("no retry/diagnostic: %v", c.LastError())
				}
				time.Sleep(time.Millisecond)
			}
			cancel()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("cleanup blocked")
			}
		})
	}
}

func TestTrafficCaptureIsolatesInterfaceFailure(t *testing.T) {
	c := testTrafficCapture("quiet")
	c.names = []string{testCaptureInterface, testBadInterface}
	original := c.command
	c.command = func(name string) *exec.Cmd {
		if name == testBadInterface {
			return exec.CommandContext(context.Background(), "/nonexistent/tcpdump")
		}
		return original(name)
	}
	sink := newCaptureTestSink()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, sink) }()
	select {
	case <-sink.ready:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("healthy interface did not start")
	}
	cancel()
	receive(t, done)
}

func TestTrafficCaptureSinkFailure(t *testing.T) {
	c := testTrafficCapture("quiet")
	sink := newCaptureTestSink()
	sink.failure = errors.New("sink failed")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Run(ctx, sink); !errors.Is(err, sink.failure) {
		t.Fatal(err)
	}
}

func TestTrafficDelayedMalformedLoss(t *testing.T) {
	c := testTrafficCapture("delayed malformed")
	sink := newCaptureTestSink()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	before := time.Now()
	go func() { done <- c.Run(ctx, sink) }()
	deadline := time.Now().Add(3 * time.Second)
	var earliest time.Time
	for {
		sink.mu.Lock()
		earliest = sink.lossStart
		sink.mu.Unlock()
		if !earliest.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			receive(t, done)
			t.Fatal("no loss notification")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	receive(t, done)
	if !earliest.Before(before.Add(-2 * time.Second)) {
		t.Fatalf("delayed loss attributed to processing time: %v", earliest)
	}
}

type captureProbeSink struct {
	*captureTestSink
	process  func() *os.Process
	inactive chan error
}

func (s *captureProbeSink) SetInterfaceState(name string, at time.Time, active bool) error {
	err := s.captureTestSink.SetInterfaceState(name, at, active)
	if !active {
		select {
		case s.inactive <- s.process().Signal(syscall.Signal(0)):
		default:
		}
	}
	return err
}

func TestTrafficClosesAvailabilityBeforeCleanup(t *testing.T) {
	c := testTrafficCapture("framing")
	original := c.command
	var command *exec.Cmd
	c.command = func(name string) *exec.Cmd { command = original(name); return command }
	sink := &captureProbeSink{
		captureTestSink: newCaptureTestSink(),
		process:         func() *os.Process { return command.Process },
		inactive:        make(chan error, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, sink) }()
	probe := receive(t, sink.inactive)
	cancel()
	receive(t, done)
	if probe != nil {
		t.Fatalf("capture stayed active until the child was reaped: %v", probe)
	}
}

func TestTrafficDropSamplingBoundary(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	sink := newCaptureTestSink()
	diagnostic := &trafficDiagnostics{start: start}
	diagnostic.noteRequest(start.Add(time.Second))
	if err := diagnostic.dropReport(
		"0 packets dropped by kernel",
		start.Add(5*time.Second),
		sink,
	); err != nil {
		t.Fatal(err)
	}
	diagnostic.noteRequest(start.Add(10 * time.Second))
	if err := diagnostic.dropReport(
		"1 packet dropped by kernel",
		start.Add(11*time.Second),
		sink,
	); err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.lossStart.After(start.Add(time.Second)) {
		t.Fatalf("lost interval between sampling and report receipt: %v", sink.lossStart)
	}
}

func TestTrafficAvailabilityCannotReactivate(t *testing.T) {
	sink := newCaptureTestSink()
	availability := &trafficAvailability{sink: sink, name: testCaptureInterface}
	start := time.Unix(1000, 0).UTC()
	if err := availability.stop(start); err != nil {
		t.Fatal(err)
	}
	if err := availability.activate(
		start.Add(time.Second),
	); !errors.Is(
		err,
		errTrafficSessionStopped,
	) {
		t.Fatal(err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.active[testCaptureInterface] {
		t.Fatal("late header reactivated stopped capture")
	}
}

func TestTrafficCaptureDrainsFinalFlush(t *testing.T) {
	c := testTrafficCapture("flush")
	sink := newCaptureTestSink()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, sink) }()
	receive(t, sink.ready)
	cancel()
	receive(t, done)
	select {
	case packet := <-sink.packets:
		if packet.Flow.Source.Port != 321 {
			t.Fatal(packet)
		}
	default:
		t.Fatal("discarded final buffered capture after cancellation")
	}
}

type capturePacketFailSink struct {
	*captureTestSink
	err error
}

func (s *capturePacketFailSink) RecordPacket(metrics.TrafficPacket) error { return s.err }
func TestTrafficCapturePacketSinkFailure(t *testing.T) {
	sentinel := errors.New("packet sink failed")
	sink := &capturePacketFailSink{captureTestSink: newCaptureTestSink(), err: sentinel}
	capture := testTrafficCapture(testPacketMode)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := capture.Run(ctx, sink); !errors.Is(err, sentinel) {
		t.Fatalf("sink failure was treated as a capture retry: %v", err)
	}
}
