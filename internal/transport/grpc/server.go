package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/daemon"
	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
)

type Options struct {
	MaxWindowSeconds int
	SendTimeout      time.Duration
	MaxSubscriptions int
}

type Server struct {
	statsv1.UnimplementedStatsServiceServer
	provider daemon.Provider
	options  Options
	clock    clock
	slots    chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewServer(provider daemon.Provider, o Options) (*Server, error) {
	if o.MaxWindowSeconds == 0 {
		o.MaxWindowSeconds = 3600
	}
	if o.SendTimeout == 0 {
		o.SendTimeout = 5 * time.Second
	}
	if o.MaxSubscriptions == 0 {
		o.MaxSubscriptions = 32
	}
	if provider == nil || o.MaxWindowSeconds < 1 || o.MaxWindowSeconds > 3600 ||
		o.SendTimeout <= 0 ||
		o.MaxSubscriptions < 1 ||
		o.MaxSubscriptions > 32 {
		return nil, fmt.Errorf("invalid server options or provider")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		provider: provider,
		options:  o,
		clock:    realClock{},
		slots:    make(chan struct{}, o.MaxSubscriptions),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// Close cancels subscriptions before the owning gRPC server begins graceful stop.
func (s *Server) Close() { s.cancel() }

type timer interface {
	C() <-chan time.Time
	Stop()
}
type clock interface {
	Now() time.Time
	NewTimer(time.Duration) timer
}
type realClock struct{}

func (realClock) Now() time.Time                 { return time.Now() }
func (realClock) NewTimer(d time.Duration) timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ *time.Timer }

func (t realTimer) C() <-chan time.Time { return t.Timer.C }
func (t realTimer) Stop()               { t.Timer.Stop() }
