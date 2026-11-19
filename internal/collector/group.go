package collector

import (
	"context"
	"fmt"
	"sync"
)

// Worker runs until cancellation or a terminal failure.
type Worker func(context.Context) error

// RunGroup runs independent workers and waits for all of them to stop.
// Measurement failures should be reported as samples, not worker returns.
func RunGroup(ctx context.Context, workers ...Worker) error {
	for i, w := range workers {
		if w == nil {
			return fmt.Errorf("worker %d: nil worker", i)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	group, cancel := context.WithCancel(ctx)
	defer cancel()
	failures := make(chan error, 1)
	var pending sync.WaitGroup
	for i, w := range workers {
		pending.Add(1)
		go func() {
			defer pending.Done()
			err := w(group)
			if group.Err() != nil {
				return
			}
			if err == nil {
				err = fmt.Errorf("unexpected successful return")
			}
			select {
			case failures <- fmt.Errorf("worker %d: %w", i, err):
				cancel()
			default:
			}
		}()
	}
	var err error
	select {
	case err = <-failures:
	case <-ctx.Done():
		err = ctx.Err()
	}
	cancel()
	pending.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
