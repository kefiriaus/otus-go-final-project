package metrics

import (
	"errors"
	"net/netip"
	"time"
)

// Endpoint preserves an absent port separately from the valid port zero.
type Endpoint struct {
	Address netip.Addr
	Port    uint16
	HasPort bool
}

// FlowKey identifies directed IP traffic. Protocol numbers are open-ended.
type FlowKey struct {
	Source      Endpoint
	Destination Endpoint
	Protocol    uint8
}

// TrafficPacket contains immutable metadata only, never captured payload.
type TrafficPacket struct {
	At        time.Time
	Interface string
	Flow      FlowKey
	IPBytes   uint64
	FlowKnown bool
	Partial   bool
}

// ProtocolTraffic counts IP bytes, including IP headers but no link headers.
type ProtocolTraffic struct {
	Protocol uint8
	Bytes    uint64
	Percent  float64
}

// FlowTraffic reports bytes per usable second, not bits per second.
type FlowTraffic struct {
	Flow           FlowKey
	BytesPerSecond float64
}

// Traffic is a window's sorted protocol list and top directed flows.
type Traffic struct {
	Protocols []ProtocolTraffic
	Flows     []FlowTraffic
}

// ErrTrafficPacketRejected marks invalid or expired packet metadata, not a broken sink.
var ErrTrafficPacketRejected = errors.New("traffic packet rejected")
