package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/config"
	"github.com/kefiriaus/otus-go-final-project/internal/daemon"
	transport "github.com/kefiriaus/otus-go-final-project/internal/transport/grpc"
)

type channelWriter struct {
	writes chan string
}

func (w channelWriter) Write(p []byte) (int, error) {
	w.writes <- string(p)
	return len(p), nil
}

func waitForCPUData(ctx context.Context, t *testing.T, runtime *daemon.Runtime) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		now := time.Now().UTC()
		window := aggregate.Window{Start: now.Add(-time.Second), End: now}
		snapshot, err := runtime.Snapshot(ctx, window)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.CPU.Summary.Data != nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("CPU data did not become ready")
		case <-ticker.C:
		}
	}
}

func waitForCPUOutput(t *testing.T, writer channelWriter) string {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case output := <-writer.writes:
			if strings.Contains(output, "user=") &&
				(strings.Contains(output, "cpu: OK") || strings.Contains(output, "cpu: PARTIAL")) &&
				!strings.Contains(output, "cpu: UNAVAILABLE") {
				return output
			}
		case <-deadline:
			t.Fatal("no usable CPU snapshot received")
		}
	}
}

func TestLinuxRuntimeToClientIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg := config.Default()
	cfg.Subsystems = config.Subsystems{CPU: true}
	runtime, err := daemon.NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(ctx) }()
	var stopRuntimeOnce sync.Once
	stopRuntime := func() {
		stopRuntimeOnce.Do(func() {
			cancel()
			select {
			case err := <-runDone:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("runtime returned %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Error("runtime did not stop")
			}
		})
	}
	defer stopRuntime()
	waitForCPUData(ctx, t, runtime)
	service, err := transport.NewServer(runtime, transport.Options{MaxWindowSeconds: cfg.MaxWindowSeconds})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	client := newCPUTestClient(t, service)
	receiveCtx, cancelReceive := context.WithCancel(context.Background())
	writer := channelWriter{writes: make(chan string, 1)}
	receiveDone := make(chan error, 1)
	options := clientOptions{intervalSeconds: 1, windowSeconds: 1}
	go func() {
		receiveDone <- receiveSnapshots(receiveCtx, client, options, writer)
	}()
	var stopReceiveOnce sync.Once
	stopReceive := func() {
		stopReceiveOnce.Do(func() {
			cancelReceive()
			select {
			case err := <-receiveDone:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("receive returned %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Error("receive did not stop")
			}
		})
	}
	defer stopReceive()
	waitForCPUOutput(t, writer)
	stopReceive()
	service.Close()
	stopRuntime()
}
