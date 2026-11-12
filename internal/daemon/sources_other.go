//go:build !linux

package daemon

import (
	"fmt"
	"runtime"

	"github.com/kefiriaus/otus-go-final-project/internal/config"
)

func platformSources(cfg config.Config) sources {
	var s sources
	for i := range s.errors {
		s.errors[i] = fmt.Errorf("collection is unavailable on %s", runtime.GOOS)
	}
	return s
}
