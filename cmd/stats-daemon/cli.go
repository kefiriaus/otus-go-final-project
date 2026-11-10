package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/kefiriaus/otus-go-final-project/internal/config"
)

func parseArgs(args []string, output io.Writer) (config.Config, error) {
	cfg := config.Default()
	flags := flag.NewFlagSet("stats-daemon", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	port := flags.Int("port", cfg.Port, "gRPC server port (1..65535)")
	path := flags.String("config", "", "JSON configuration file (optional)")
	var usage bytes.Buffer
	flags.Usage = func() {
		fmt.Fprintln(&usage, "Usage: stats-daemon [-port PORT] [-config FILE]")
		flags.SetOutput(&usage)
		flags.PrintDefaults()
		flags.SetOutput(io.Discard)
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(output, usage.String())
		}
		return config.Config{}, err
	}
	if flags.NArg() != 0 {
		return config.Config{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	configSet := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "config" {
			configSet = true
		}
	})
	if configSet {
		if *path == "" {
			return config.Config{}, fmt.Errorf("config path must not be empty")
		}
		file, err := os.Open(*path)
		if err != nil {
			return config.Config{}, fmt.Errorf("config %q: %w", *path, err)
		}
		defer file.Close()
		cfg, err = config.LoadJSON(file)
		if err != nil {
			return config.Config{}, fmt.Errorf("config %q: %w", *path, err)
		}
	}
	cfg.Port = *port
	if err := cfg.Validate(); err != nil {
		return config.Config{}, err
	}
	return cfg, nil
}

func run(args []string, stdout, stderr io.Writer) int {
	_, err := parseArgs(args, stdout)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "stats-daemon: %v\n", err)
		return 2
	}
	fmt.Fprintln(stderr, "stats-daemon: scaffold only; server is not implemented")
	return 1
}
