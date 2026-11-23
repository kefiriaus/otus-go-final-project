package daemon

import (
	"github.com/kefiriaus/otus-go-final-project/internal/collector"
	"github.com/kefiriaus/otus-go-final-project/internal/config"
)

func platformSources(cfg config.Config) sources {
	var s sources
	if cfg.Subsystems.LoadAverage {
		s.load = collector.NewLoadAverageSource()
	}
	if cfg.Subsystems.CPU {
		s.cpu = collector.NewCPUSource()
	}
	if cfg.Subsystems.DiskIO {
		s.disk = collector.NewDiskIOSource()
	}
	if cfg.Subsystems.Filesystems {
		s.filesystems = collector.NewFilesystemsSource()
	}
	if cfg.Subsystems.Connections {
		s.connections, s.errors[5] = collector.NewConnectionsSource(
			collector.DefaultConnectionsOptions(),
		)
		if s.errors[5] != nil {
			s.connections = nil
		}
	}
	if cfg.Subsystems.Traffic {
		s.traffic, s.errors[4] = collector.NewTrafficCapture(
			collector.DefaultTrafficCaptureOptions(),
		)
		if s.errors[4] != nil {
			s.traffic = nil
		}
	}
	return s
}
