package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/config"
	"github.com/kefiriaus/otus-go-final-project/internal/daemon"
	transport "github.com/kefiriaus/otus-go-final-project/internal/transport/grpc"
	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/grpc"
)

type runtimeService interface {
	daemon.Provider
	Run(context.Context) error
}

func serve(ctx context.Context, cfg config.Config) error {
	return serveWith(ctx, cfg, net.Listen, func(c config.Config) (runtimeService, error) { return daemon.NewRuntime(c) })
}
func serveWith(ctx context.Context, cfg config.Config, listen func(string, string) (net.Listener, error), makeRuntime func(config.Config) (runtimeService, error)) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()
	runtime, err := makeRuntime(cfg)
	if err != nil {
		return err
	}
	service, err := transport.NewServer(runtime, transport.Options{MaxWindowSeconds: cfg.MaxWindowSeconds})
	if err != nil {
		return err
	}
	defer service.Close()
	server := grpc.NewServer()
	statsv1.RegisterStatsServiceServer(server, service)
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	serving := make(chan error, 1)
	collecting := make(chan error, 1)
	go func() { defer close(serving); serving <- server.Serve(listener) }()
	go func() { defer close(collecting); collecting <- runtime.Run(child) }()
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-serving:
		if err == nil {
			err = errors.New("gRPC server stopped unexpectedly")
		}
	case err = <-collecting:
		if err == nil {
			err = errors.New("collection stopped unexpectedly")
		}
	}
	cancel()
	service.Close()
	stopped := make(chan struct{})
	go func() { server.GracefulStop(); close(stopped) }()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-stopped:
	case <-timer.C:
		server.Stop()
		<-stopped
	}
	<-serving
	<-collecting
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
