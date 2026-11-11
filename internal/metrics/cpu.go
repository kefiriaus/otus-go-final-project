package metrics

// CPUCounters contains cumulative aggregate CPU accounting ticks. User and Nice
// already include guest time, so separate guest counters must not be added again.
type CPUCounters struct {
	User    uint64
	Nice    uint64
	System  uint64
	Idle    uint64
	IOWait  uint64
	IRQ     uint64
	SoftIRQ uint64
	Steal   uint64
}

// CPUUsage is the distribution of usable CPU counter deltas over a window.
// User includes nice; system includes IRQ and softIRQ. Idle excludes I/O wait.
type CPUUsage struct {
	UserPercent   float64
	SystemPercent float64
	IdlePercent   float64
	IOWaitPercent float64
	StealPercent  float64
}
