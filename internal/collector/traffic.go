package collector

import (
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
	"time"
)

// TrafficSink callbacks must return promptly and be safe for concurrent use.
// TrafficHistory satisfies this contract without depending on collector.
type TrafficSink interface {
	RecordPacket(metrics.TrafficPacket) error
	SetInterfaceState(string, time.Time, bool) error
	MarkLoss(time.Time, time.Time) error
	Advance(time.Time) error
}
