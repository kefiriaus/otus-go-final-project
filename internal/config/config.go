package config

import "fmt"

// Config is the validated startup configuration of the daemon.
type Config struct {
	Port             int
	MaxWindowSeconds int
	Subsystems       Subsystems
}

// Subsystems selects independently enabled collectors.
type Subsystems struct {
	LoadAverage bool
	CPU         bool
	DiskIO      bool
	Filesystems bool
	Traffic     bool
	Connections bool
}

// Default returns an independent configuration with all collectors enabled.
func Default() Config {
	return Config{Port: 50051, MaxWindowSeconds: 3600, Subsystems: Subsystems{
		LoadAverage: true, CPU: true, DiskIO: true, Filesystems: true, Traffic: true, Connections: true,
	}}
}

// Validate checks startup limits without accessing files or sockets.
func (c Config) Validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("port %d: must be in 1..65535", c.Port)
	}
	if c.MaxWindowSeconds < 1 || c.MaxWindowSeconds > 3600 {
		return fmt.Errorf("max_window_seconds %d: must be in 1..3600", c.MaxWindowSeconds)
	}
	return nil
}
