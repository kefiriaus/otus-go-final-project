// Package daemon owns collection and protobuf-independent snapshots.
package daemon

import (
	"context"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

type Section[T any] struct {
	Enabled bool
	Summary aggregate.Summary[T]
	Detail  string
}

type Snapshot struct {
	Window      aggregate.Window
	GeneratedAt time.Time
	LoadAverage Section[metrics.LoadAverage]
	CPU         Section[metrics.CPUUsage]
	DiskIO      Section[[]metrics.DiskIO]
	Filesystems Section[[]metrics.FilesystemUsage]
	Traffic     Section[metrics.Traffic]
	Connections Section[metrics.Connections]
}

type Provider interface {
	Snapshot(context.Context, aggregate.Window) (Snapshot, error)
}
