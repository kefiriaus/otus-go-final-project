package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
)

type clientOptions struct {
	address         string
	intervalSeconds uint32
	windowSeconds   uint32
}

type receiveFunc func(context.Context, clientOptions, io.Writer) error

func parseArgs(args []string, output io.Writer) (clientOptions, error) {
	options := clientOptions{address: "127.0.0.1:50051", intervalSeconds: 5, windowSeconds: 15}
	flags := flag.NewFlagSet("stats-client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	address := flags.String("address", options.address, "daemon address (host:port)")
	interval := flags.Int(
		"interval",
		int(options.intervalSeconds),
		"subscription interval in seconds (1..3600)",
	)
	window := flags.Int(
		"window",
		int(options.windowSeconds),
		"aggregation window in seconds (1..3600)",
	)
	var usage bytes.Buffer
	flags.Usage = func() {
		fmt.Fprintln(&usage, "Usage: stats-client [-address HOST:PORT] [-interval N] [-window M]")
		flags.SetOutput(&usage)
		flags.PrintDefaults()
		flags.SetOutput(io.Discard)
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(output, usage.String())
		}
		return clientOptions{}, err
	}
	if flags.NArg() != 0 {
		return clientOptions{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *address == "" {
		return clientOptions{}, fmt.Errorf("address must not be empty")
	}
	if *interval < 1 || *interval > 3600 {
		return clientOptions{}, fmt.Errorf("interval must be in 1..3600")
	}
	if *window < 1 || *window > 3600 {
		return clientOptions{}, fmt.Errorf("window must be in 1..3600")
	}
	options.address = *address
	options.intervalSeconds = uint32(*interval)
	options.windowSeconds = uint32(*window)
	return options, nil
}

func runContext(
	ctx context.Context,
	args []string,
	stdout, stderr io.Writer,
	receive receiveFunc,
) int {
	options, err := parseArgs(args, stdout)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "stats-client: %v\n", err)
		return 2
	}
	if err := receive(ctx, options, stdout); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(stderr, "stats-client: %v\n", err)
		return 1
	}
	return 0
}
