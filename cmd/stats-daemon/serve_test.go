package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/config"
	"github.com/kefiriaus/otus-go-final-project/internal/daemon"
	"google.golang.org/grpc/test/bufconn"
)

func TestRunServingResult(t *testing.T) {
	for _, test := range []struct {
		err  error
		code int
	}{{nil, 0}, {errors.New("bind failure"), 1}, {context.Canceled, 0}} {
		var stdout, stderr bytes.Buffer
		called := false
		code := runContext(context.Background(), []string{"-port", "12345"}, &stdout, &stderr, func(_ context.Context, cfg config.Config) error {
			called = true
			if cfg.Port != 12345 {
				t.Fatal(cfg)
			}
			return test.err
		})
		if !called || code != test.code {
			t.Fatalf("called=%v code=%d", called, code)
		}
	}
}
func TestHelpSkipsServing(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"-port", "0"}} {
		var out bytes.Buffer
		runContext(context.Background(), args, &out, &out, func(context.Context, config.Config) error { t.Fatal("unexpected serving"); return nil })
	}
}

type fakeRuntime struct {
	started chan struct{}
	failure error
}

func (r *fakeRuntime) Run(ctx context.Context) error {
	close(r.started)
	if r.failure != nil {
		return r.failure
	}
	<-ctx.Done()
	return ctx.Err()
}
func (r *fakeRuntime) Snapshot(context.Context, aggregate.Window) (daemon.Snapshot, error) {
	return daemon.Snapshot{}, nil
}
func TestServeCancellationAndFailure(t *testing.T) {
	for _, failure := range []error{nil, errors.New("runtime failed")} {
		t.Run(fmtError(failure), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			listener := bufconn.Listen(1024)
			r := &fakeRuntime{started: make(chan struct{}), failure: failure}
			done := make(chan error, 1)
			go func() {
				done <- serveWith(ctx, config.Default(), func(network, address string) (net.Listener, error) {
					if address != ":50051" {
						t.Error(address)
					}
					return listener, nil
				}, func(config.Config) (runtimeService, error) { return r, nil })
			}()
			select {
			case <-r.started:
			case <-time.After(time.Second):
				t.Fatal("not started")
			}
			if failure == nil {
				cancel()
			}
			select {
			case err := <-done:
				if failure == nil && !errors.Is(err, context.Canceled) || failure != nil && !errors.Is(err, failure) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("shutdown stuck")
			}
		})
	}
}
func fmtError(err error) string {
	if err == nil {
		return "cancel"
	}
	return "failure"
}
func TestListenFailureStartsNoRuntime(t *testing.T) {
	sentinel := errors.New("bind failed")
	err := serveWith(context.Background(), config.Default(), func(string, string) (net.Listener, error) { return nil, sentinel }, func(config.Config) (runtimeService, error) {
		t.Fatal("constructed runtime after listen failure")
		return nil, nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}
