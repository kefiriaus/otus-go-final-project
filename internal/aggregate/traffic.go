package aggregate

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

// TrafficHistoryOptions bounds retained packet metadata and distinct flow keys.
// Hard upper limits are 1,048,576 packets and 65,536 flows.
type TrafficHistoryOptions struct {
	MaxPackets int
	MaxFlows   int
}

// DefaultTrafficHistoryOptions provides bounded defaults for shared capture.
func DefaultTrafficHistoryOptions() TrafficHistoryOptions { return TrafficHistoryOptions{65536, 4096} }

type (
	trafficSpan      struct{ start, end time.Time }
	trafficInterface struct {
		active      bool
		start, last time.Time
		closed      []trafficSpan
	}
)

// TrafficHistory stores metadata only. Reads never consume another client's data.
// Construct with NewTrafficHistory; all methods are safe for concurrent use.
type TrafficHistory struct {
	mu                  sync.Mutex
	packets             []metrics.TrafficPacket
	head, size          int
	flows               map[metrics.FlowKey]int
	maxFlows, maxStates int
	window, retention   time.Duration
	watermark           time.Time
	interfaces          map[string]*trafficInterface
	loss                trafficSpan
}

// NewTrafficHistory fixes the selected interface set and all storage bounds.
func NewTrafficHistory(
	maxWindowSeconds int,
	interfaces []string,
	options TrafficHistoryOptions,
) (*TrafficHistory, error) {
	if maxWindowSeconds < 1 || maxWindowSeconds > 3600 || options.MaxPackets < 1 ||
		options.MaxPackets > 1<<20 ||
		options.MaxFlows < 1 ||
		options.MaxFlows > 65536 ||
		len(interfaces) < 1 ||
		len(interfaces) > 8 {
		return nil, fmt.Errorf("invalid traffic history limits or interfaces")
	}
	h := &TrafficHistory{
		packets:    make([]metrics.TrafficPacket, options.MaxPackets),
		flows:      make(map[metrics.FlowKey]int),
		maxFlows:   options.MaxFlows,
		maxStates:  2*(maxWindowSeconds+2) + 4,
		window:     time.Duration(maxWindowSeconds) * time.Second,
		retention:  time.Duration(maxWindowSeconds+2) * time.Second,
		interfaces: make(map[string]*trafficInterface),
	}
	for _, name := range interfaces {
		if name == "" || len(name) > 64 || strings.ContainsAny(name, "/\x00\n\r") {
			return nil, fmt.Errorf("invalid traffic interface")
		}
		if _, ok := h.interfaces[name]; ok {
			return nil, fmt.Errorf("duplicate traffic interface")
		}
		h.interfaces[name] = &trafficInterface{}
	}
	return h, nil
}

func (h *TrafficHistory) lossLocked(start, end time.Time) {
	if h.loss.start.IsZero() || start.Before(h.loss.start) {
		h.loss.start = start.UTC()
	}
	if end.After(h.loss.end) {
		h.loss.end = end.UTC()
	}
}

// MarkLoss conservatively coalesces losses into one bounded range.
func (h *TrafficHistory) MarkLoss(start, end time.Time) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if start.IsZero() || !start.Before(end) {
		return fmt.Errorf("invalid traffic loss interval")
	}
	h.lossLocked(start, end)
	return nil
}

func (h *TrafficHistory) release(p metrics.TrafficPacket) {
	if p.FlowKnown {
		h.flows[p.Flow]--
		if h.flows[p.Flow] == 0 {
			delete(h.flows, p.Flow)
		}
	}
}

// Advance updates the explicit availability watermark and expires old metadata.
func (h *TrafficHistory) Advance(now time.Time) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if now.IsZero() || (!h.watermark.IsZero() && now.Before(h.watermark)) {
		return fmt.Errorf("traffic watermark must not decrease")
	}
	h.watermark = now.UTC()
	cutoff := now.Add(-h.retention)
	kept, old := 0, h.size
	for i := 0; i < old; i++ {
		p := h.packets[(h.head+i)%len(h.packets)]
		if p.At.Before(cutoff) {
			h.release(p)
			continue
		}
		h.packets[(h.head+kept)%len(h.packets)] = p
		kept++
	}
	for i := kept; i < old; i++ {
		h.packets[(h.head+i)%len(h.packets)] = metrics.TrafficPacket{}
	}
	h.size = kept
	for _, state := range h.interfaces {
		n := 0
		for _, span := range state.closed {
			if !span.end.After(cutoff) {
				continue
			}
			if span.start.Before(cutoff) {
				span.start = cutoff
			}
			state.closed[n] = span
			n++
		}
		clear(state.closed[n:])
		state.closed = state.closed[:n]
		if state.active && state.start.Before(cutoff) {
			state.start = cutoff
		}
	}
	if !h.loss.end.After(cutoff) {
		h.loss = trafficSpan{}
	} else if h.loss.start.Before(cutoff) {
		h.loss.start = cutoff
	}
	return nil
}

// SetInterfaceState records a capture lifecycle transition, not packet activity.
func (h *TrafficHistory) SetInterfaceState(name string, at time.Time, active bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	state, ok := h.interfaces[name]
	if !ok || at.IsZero() || (!state.last.IsZero() && at.Before(state.last)) ||
		(!h.watermark.IsZero() && at.Before(h.watermark.Add(-h.retention))) {
		return fmt.Errorf("unknown interface or stale traffic state")
	}
	at = at.UTC()
	state.last = at
	if state.active == active {
		return nil
	}
	if active {
		state.start = at
	} else if at.After(state.start) {
		if len(state.closed) == h.maxStates {
			lost := state.closed[0]
			h.lossLocked(lost.start, lost.end)
			copy(state.closed, state.closed[1:])
			state.closed = state.closed[:len(state.closed)-1]
		}
		state.closed = append(state.closed, trafficSpan{state.start, at})
	}
	state.active = active
	return nil
}

// RecordPacket retains bounded metadata; capacity/flow losses mark partial data.
func (h *TrafficHistory) RecordPacket(packet metrics.TrafficPacket) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.interfaces[packet.Interface]
	if !ok {
		return fmt.Errorf("unknown traffic interface %q", packet.Interface)
	}
	source, dest := packet.Flow.Source, packet.Flow.Destination
	if !ok || h.watermark.IsZero() || packet.At.IsZero() ||
		packet.At.Before(h.watermark.Add(-h.retention)) ||
		packet.At.After(h.watermark.Add(5*time.Second)) ||
		!source.Address.IsValid() ||
		!dest.Address.IsValid() ||
		source.Address.Zone() != "" ||
		dest.Address.Zone() != "" ||
		source.Address.Is4() != dest.Address.Is4() ||
		source.HasPort != dest.HasPort ||
		packet.IPBytes < 20 ||
		packet.IPBytes > 65575 ||
		(source.Address.Is6() && packet.IPBytes < 40) ||
		((packet.Flow.Protocol == 6 || packet.Flow.Protocol == 17) && packet.FlowKnown && !source.HasPort) {
		return fmt.Errorf("%w: invalid metadata or timestamp", metrics.ErrTrafficPacketRejected)
	}
	packet.At = packet.At.UTC()
	if !source.HasPort {
		packet.Flow.Source.Port = 0
		packet.Flow.Destination.Port = 0
	}
	if h.size == len(h.packets) {
		lost := h.packets[h.head]
		h.release(lost)
		h.lossLocked(lost.At, lost.At.Add(time.Nanosecond))
		h.head = (h.head + 1) % len(h.packets)
		h.size--
	}
	if packet.FlowKnown {
		if _, exists := h.flows[packet.Flow]; !exists && len(h.flows) == h.maxFlows {
			packet.FlowKnown = false
			packet.Partial = true
		} else {
			h.flows[packet.Flow]++
		}
	}
	if !packet.FlowKnown {
		packet.Partial = true
	}
	h.packets[(h.head+h.size)%len(h.packets)] = packet
	h.size++
	return nil
}

func spanIntersection(span trafficSpan, w Window) (trafficSpan, bool) {
	if span.start.Before(w.Start) {
		span.start = w.Start
	}
	if span.end.After(w.End) {
		span.end = w.End
	}
	return span, span.start.Before(span.end)
}

func compareEndpoint(a, b metrics.Endpoint) int {
	if c := strings.Compare(a.Address.String(), b.Address.String()); c != 0 {
		return c
	}
	if a.HasPort != b.HasPort {
		if !a.HasPort {
			return -1
		}
		return 1
	}
	if a.Port < b.Port {
		return -1
	}
	if a.Port > b.Port {
		return 1
	}
	return 0
}

func compareFlow(a, b metrics.FlowKey) int {
	if c := compareEndpoint(a.Source, b.Source); c != 0 {
		return c
	}
	if c := compareEndpoint(a.Destination, b.Destination); c != 0 {
		return c
	}
	return int(a.Protocol) - int(b.Protocol)
}

type trafficEdge struct {
	at    time.Time
	delta int
}

func (h *TrafficHistory) collectTrafficBytes(
	window Window,
	available map[string][]trafficSpan,
) ([256]uint64, map[metrics.FlowKey]uint64, uint64, bool, error) {
	var protocols [256]uint64
	flowBytes := make(map[metrics.FlowKey]uint64)
	total := uint64(0)
	partial := false
	for i := 0; i < h.size; i++ {
		p := h.packets[(h.head+i)%len(h.packets)]
		if p.At.Before(window.Start) || !p.At.Before(window.End) || !p.At.Before(h.watermark) {
			continue
		}
		spans := available[p.Interface]
		index := sort.Search(len(spans), func(i int) bool { return spans[i].end.After(p.At) })
		if index >= len(spans) || p.At.Before(spans[index].start) {
			partial = true
			continue
		}
		if total > math.MaxUint64-p.IPBytes || protocols[p.Flow.Protocol] > math.MaxUint64-p.IPBytes {
			return protocols, nil, 0, false, fmt.Errorf("traffic byte overflow")
		}
		total += p.IPBytes
		protocols[p.Flow.Protocol] += p.IPBytes
		partial = partial || p.Partial
		if p.FlowKnown {
			if flowBytes[p.Flow] > math.MaxUint64-p.IPBytes {
				return protocols, nil, 0, false, fmt.Errorf("flow byte overflow")
			}
			flowBytes[p.Flow] += p.IPBytes
		}
	}
	return protocols, flowBytes, total, partial, nil
}

func buildTrafficData(
	protocols [256]uint64,
	flowBytes map[metrics.FlowKey]uint64,
	total uint64,
	covered time.Duration,
	topK int,
) metrics.Traffic {
	data := metrics.Traffic{
		Protocols: make([]metrics.ProtocolTraffic, 0),
		Flows:     make([]metrics.FlowTraffic, 0),
	}
	for protocol, n := range protocols {
		if n > 0 {
			data.Protocols = append(data.Protocols, metrics.ProtocolTraffic{
				Protocol: uint8(protocol), Bytes: n, Percent: float64(n) / float64(total) * 100,
			})
		}
	}
	slices.SortFunc(data.Protocols, func(a, b metrics.ProtocolTraffic) int {
		if a.Bytes != b.Bytes {
			return cmp.Compare(b.Bytes, a.Bytes)
		}
		return int(a.Protocol) - int(b.Protocol)
	})
	for flow, n := range flowBytes {
		data.Flows = append(data.Flows, metrics.FlowTraffic{
			Flow: flow, BytesPerSecond: float64(n) / covered.Seconds(),
		})
	}
	slices.SortFunc(data.Flows, func(a, b metrics.FlowTraffic) int {
		if a.BytesPerSecond != b.BytesPerSecond {
			return cmp.Compare(b.BytesPerSecond, a.BytesPerSecond)
		}
		return compareFlow(a.Flow, b.Flow)
	})
	if len(data.Flows) > topK {
		data.Flows = data.Flows[:topK]
	}
	return data
}

// Snapshot calculates exact packet-window counts and usable-time flow rates.
// topK must be 1..MaxFlows; the application default is 20.
func (h *TrafficHistory) Snapshot(window Window, topK int) (Summary[metrics.Traffic], error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := window.validate(); err != nil {
		return Summary[metrics.Traffic]{}, err
	}
	if window.End.Sub(window.Start) > h.window || topK < 1 || topK > h.maxFlows {
		return Summary[metrics.Traffic]{}, fmt.Errorf("invalid traffic window or topK")
	}
	var edges []trafficEdge
	available := make(map[string][]trafficSpan, len(h.interfaces))
	for name, state := range h.interfaces {
		add := func(span trafficSpan) {
			if span.end.After(h.watermark) {
				span.end = h.watermark
			}
			if clipped, ok := spanIntersection(span, window); ok {
				available[name] = append(available[name], clipped)
				edges = append(edges, trafficEdge{clipped.start, 1}, trafficEdge{clipped.end, -1})
			}
		}
		for _, span := range state.closed {
			add(span)
		}
		if state.active {
			add(trafficSpan{state.start, h.watermark})
		}
	}
	slices.SortFunc(edges, func(a, b trafficEdge) int { return a.at.Compare(b.at) })
	var covered, complete time.Duration
	count := 0
	var previous, last time.Time
	for _, edge := range edges {
		if count > 0 {
			covered += edge.at.Sub(previous)
			last = edge.at
		}
		if count == len(h.interfaces) {
			complete += edge.at.Sub(previous)
		}
		count += edge.delta
		previous = edge.at
	}
	if covered == 0 {
		return Summary[metrics.Traffic]{}, nil
	}
	partial := covered < window.End.Sub(window.Start) || complete < covered
	if _, overlap := spanIntersection(h.loss, window); !h.loss.start.IsZero() && overlap {
		partial = true
	}
	protocols, flowBytes, total, packetPartial, err := h.collectTrafficBytes(window, available)
	if err != nil {
		return Summary[metrics.Traffic]{}, err
	}
	partial = partial || packetPartial
	data := buildTrafficData(protocols, flowBytes, total, covered, topK)
	timestamp := last.UTC()
	return Summary[metrics.Traffic]{
		Data:          &data,
		CoverageRatio: float64(covered) / float64(window.End.Sub(window.Start)),
		LastSampleAt:  &timestamp,
		Partial:       partial,
	}, nil
}
