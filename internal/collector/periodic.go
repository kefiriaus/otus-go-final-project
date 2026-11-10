package collector

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Source obtains one measurement and must return promptly when ctx is canceled.
// Returned values, including referenced storage, must not be mutated afterwards.
type Source[T any] interface {
	Collect(context.Context) (T, error)
}

// Sample records an attempt. Value is usable only when Err is nil.
type Sample[T any] struct {
	StartedAt  time.Time
	FinishedAt time.Time
	Value      T
	Err        error
}

// Options controls periodic collection.
type Options struct {
	Interval time.Duration
	Timeout  time.Duration
}

// DefaultOptions returns the production sampling schedule.
func DefaultOptions() Options { return Options{Interval: time.Second, Timeout: 900 * time.Millisecond} }

// RunPeriodic collects immediately and on a fixed schedule, skipping busy ticks.
// It never closes output. Delivery can block this runner until ctx is canceled.
func RunPeriodic[T any](ctx context.Context, source Source[T], options Options, output chan<- Sample[T]) error {
	if options.Interval <= 0 || options.Timeout <= 0 {
		return fmt.Errorf("collection interval and timeout must be positive")
	}
	if output == nil || source == nil {
		return fmt.Errorf("source and output must not be nil")
	}
	ticker := time.NewTicker(options.Interval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		sample := Sample[T]{StartedAt: time.Now().UTC()}
		attempt, cancel := context.WithTimeout(ctx, options.Timeout)
		sample.Value, sample.Err = source.Collect(attempt)
		sample.FinishedAt = time.Now().UTC()
		if err := attempt.Err(); err != nil {
			sample.Err = errors.Join(sample.Err, err)
		}
		cancel()
		if err := ctx.Err(); err != nil {
			return err
		}
		if sample.Err != nil {
			var zero T
			sample.Value = zero
		}
		select {
		case output <- sample:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-ticker.C:
		default:
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
