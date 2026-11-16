//go:build darwin

package daemon

import (
	"fmt"
	"runtime"

	"github.com/kefiriaus/otus-go-final-project/internal/collector"
	"github.com/kefiriaus/otus-go-final-project/internal/config"
)

func platformSources(cfg config.Config) sources {
	var s sources
	for i := range s.errors {
		s.errors[i] = fmt.Errorf("collection is unavailable on %s", runtime.GOOS)
	}
	if cfg.Subsystems.Filesystems {
		s.filesystems = collector.NewFilesystemsSource()
		s.errors[3] = nil
	}
	return s
}
