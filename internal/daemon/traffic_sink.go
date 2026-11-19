package daemon

import (
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

type trafficSink struct{ store *Store }

func (s *Store) advanceLocked(at time.Time) error {
	if !at.After(s.trafficWatermark) {
		return nil
	}
	if err := s.traffic.Advance(at); err != nil {
		return err
	}
	s.trafficWatermark = at
	return nil
}

func (a trafficSink) Advance(at time.Time) error {
	s := a.store
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.advanceLocked(at)
}

func (a trafficSink) SetInterfaceState(name string, at time.Time, active bool) error {
	s := a.store
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.traffic.SetInterfaceState(name, at, active)
}

func (a trafficSink) MarkLoss(start, end time.Time) error {
	s := a.store
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.traffic.MarkLoss(start, end)
}

func (a trafficSink) RecordPacket(p metrics.TrafficPacket) error {
	s := a.store
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.traffic.RecordPacket(p)
}
