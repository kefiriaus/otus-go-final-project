package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func receive(ctx context.Context, options clientOptions, output io.Writer) error {
	connection, err := grpc.NewClient(
		options.address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("create gRPC client: %w", err)
	}
	defer connection.Close()
	return receiveSnapshots(ctx, statsv1.NewStatsServiceClient(connection), options, output)
}

func receiveSnapshots(
	ctx context.Context,
	client statsv1.StatsServiceClient,
	options clientOptions,
	output io.Writer,
) error {
	stream, err := client.Subscribe(ctx, &statsv1.SubscribeRequest{
		IntervalSeconds: options.intervalSeconds,
		WindowSeconds:   options.windowSeconds,
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("subscribe: %w", err)
	}
	for {
		snapshot, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("receive snapshot: %w", err)
		}
		formatted, err := formatCPU(snapshot)
		if err != nil {
			return fmt.Errorf("format snapshot: %w", err)
		}
		n, err := io.WriteString(output, formatted)
		if err != nil {
			return fmt.Errorf("write snapshot: %w", err)
		}
		if n != len(formatted) {
			return fmt.Errorf("write snapshot: %w", io.ErrShortWrite)
		}
	}
}
