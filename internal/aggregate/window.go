package aggregate

import (
	"fmt"
	"time"
)

// Window is a half-open UTC time range.
type Window struct {
	Start time.Time
	End   time.Time
}

func (w Window) validate() error {
	if w.Start.IsZero() || w.End.IsZero() || !w.Start.Before(w.End) || w.End.After(w.Start.Add(time.Hour)) {
		return fmt.Errorf("window requires nonzero timestamps and a duration in (0, 3600s]")
	}
	return nil
}

// NewWindow constructs an M-second window within the server limit.
func NewWindow(end time.Time, seconds, maxSeconds int) (Window, error) {
	if maxSeconds < 1 || maxSeconds > 3600 || seconds < 1 || seconds > maxSeconds || end.IsZero() {
		return Window{}, fmt.Errorf("invalid window duration, limit, or end")
	}
	return Window{Start: end.Add(-time.Duration(seconds) * time.Second).UTC(), End: end.UTC()}, nil
}

// Schedule is an immutable request-relative schedule constructed by NewSchedule.
type Schedule struct {
	First         time.Time
	Interval      time.Duration
	WindowSeconds int
}

// NewSchedule validates request timing without accessing history.
func NewSchedule(acceptedAt time.Time, intervalSeconds, windowSeconds, maxSeconds int) (Schedule, error) {
	if intervalSeconds < 1 || intervalSeconds > 3600 || acceptedAt.IsZero() {
		return Schedule{}, fmt.Errorf("invalid subscription interval or acceptance time")
	}
	if _, err := NewWindow(acceptedAt, windowSeconds, maxSeconds); err != nil {
		return Schedule{}, err
	}
	return Schedule{First: acceptedAt.Add(time.Duration(windowSeconds) * time.Second).UTC(), Interval: time.Duration(intervalSeconds) * time.Second, WindowSeconds: windowSeconds}, nil
}

// NextAfter returns the first deadline strictly after now, skipping missed ticks.
func (s Schedule) NextAfter(now time.Time) time.Time {
	if now.Before(s.First) {
		return s.First.UTC()
	}
	// Reduce whole Unix seconds before subtracting to avoid duration overflow.
	period := int64(s.Interval / time.Second)
	seconds := (now.Unix()%period - s.First.Unix()%period + period) % period
	elapsed := time.Duration(seconds)*time.Second + time.Duration(now.Nanosecond()-s.First.Nanosecond())
	if elapsed < 0 {
		elapsed += s.Interval
	}
	return now.Add(s.Interval - elapsed).UTC()
}

// WindowAt returns the window associated with a scheduled response deadline.
func (s Schedule) WindowAt(end time.Time) Window {
	return Window{Start: end.Add(-time.Duration(s.WindowSeconds) * time.Second).UTC(), End: end.UTC()}
}
