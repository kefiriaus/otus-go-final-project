package metrics

// DiskIOCounters retains raw completed-I/O counters for one device identity.
// Sector counters use the kernel's fixed 512-byte accounting units.
type DiskIOCounters struct {
	Identity        string
	ReadsCompleted  uint64
	WritesCompleted uint64
	SectorsRead     uint64
	SectorsWritten  uint64
}

// DiskIOReading preserves an individual failure without losing other devices.
// Counters is usable only when Err is nil.
type DiskIOReading struct {
	Device   string
	Counters DiskIOCounters
	Err      error
}

// DiskIOObservation is a bounded enumeration of whole-device readings.
// Consumers must inspect per-device errors even when collection itself succeeds.
type DiskIOObservation struct{ Readings []DiskIOReading }

// DiskIO contains window rates for one device, without combining device layers.
type DiskIO struct {
	Device             string
	TransfersPerSecond float64
	KibibytesPerSecond float64
}
