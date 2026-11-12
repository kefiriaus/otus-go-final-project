package grpc

import (
	"context"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Subscribe(req *statsv1.SubscribeRequest, stream grpc.ServerStreamingServer[statsv1.Snapshot]) error {
	// Validate uint32 values before converting to int, including on 32-bit hosts.
	if req == nil || req.IntervalSeconds < 1 || req.IntervalSeconds > 3600 || req.WindowSeconds < 1 || req.WindowSeconds > uint32(s.options.MaxWindowSeconds) {
		return status.Error(codes.InvalidArgument, "N must be 1..3600 and M must be 1..server maximum")
	}
	accepted := s.clock.Now()
	schedule, err := aggregate.NewSchedule(accepted, int(req.IntervalSeconds), int(req.WindowSeconds), s.options.MaxWindowSeconds)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return status.Error(codes.ResourceExhausted, "subscription limit reached")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	// Exactly one sender lives for this RPC. Returning the handler cancels the
	// transport stream, allowing an in-flight Send to unwind after a timeout.
	jobs := make(chan *statsv1.Snapshot, 1)
	results := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case v := <-jobs:
				if ctx.Err() != nil {
					return
				}
				err := stream.Send(v)
				select {
				case results <- err:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		}
	}()
	next := s.clock.NewTimer(schedule.First.Sub(accepted))
	defer func() { next.Stop() }()
	var sendTimer timer
	defer func() {
		if sendTimer != nil {
			sendTimer.Stop()
		}
	}()
	var sending bool
	var pending *statsv1.Snapshot
	for {
		var timeout <-chan time.Time
		if sendTimer != nil {
			timeout = sendTimer.C()
		}
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-timeout:
			return status.Error(codes.DeadlineExceeded, "snapshot send timed out")
		case err := <-results:
			sendTimer.Stop()
			sendTimer = nil
			sending = false
			if err != nil {
				return err
			}
		case <-next.C():
			now := s.clock.Now()
			deadline := schedule.NextAfter(now).Add(-schedule.Interval)
			if now.Before(schedule.First) {
				deadline = schedule.First
			}
			value, err := s.provider.Snapshot(ctx, schedule.WindowAt(deadline))
			if err != nil {
				if ctx.Err() != nil {
					return status.FromContextError(ctx.Err()).Err()
				}
				return status.Error(codes.Internal, "snapshot construction failed")
			}
			value.GeneratedAt = s.clock.Now().UTC()
			pending, err = mapSnapshot(value)
			if err != nil {
				return status.Error(codes.Internal, "invalid snapshot")
			}
			after := s.clock.Now()
			next = s.clock.NewTimer(schedule.NextAfter(after).Sub(after))
		}
		if !sending && pending != nil {
			jobs <- pending
			pending = nil
			sending = true
			sendTimer = s.clock.NewTimer(s.options.SendTimeout)
		}
	}
}
