// Package metrics defines system measurements independently of their sources
// and transport representation.
package metrics

// LoadAverage contains the system's unnormalized 1/5/15-minute load indicators.
// The same representation holds their time-weighted means over a client window.
type LoadAverage struct {
	Load1  float64
	Load5  float64
	Load15 float64
}
