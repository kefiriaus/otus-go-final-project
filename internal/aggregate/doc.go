// Package aggregate stores bounded interval history and computes window statistics.
// Each series owns its history; client reads do not consume measurements.
// Public timestamps use UTC and windows are half-open. Values and any referenced
// storage are immutable after insertion and after reading. The package has no
// dependency on collector, configuration, or transport types.
package aggregate
