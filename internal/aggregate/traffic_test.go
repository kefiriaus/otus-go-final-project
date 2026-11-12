package aggregate

import (
	"net/netip"
	"sync"
	"testing"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func trafficPacket(atTime float64, source string, port uint16, protocol uint8, bytes uint64) metrics.TrafficPacket {
	return metrics.TrafficPacket{At: at(atTime), Interface: "eth0", Flow: metrics.FlowKey{Source: metrics.Endpoint{Address: netip.MustParseAddr(source), HasPort: protocol == 6 || protocol == 17, Port: port}, Destination: metrics.Endpoint{Address: netip.MustParseAddr("198.51.100.1"), HasPort: protocol == 6 || protocol == 17, Port: 53}, Protocol: protocol}, IPBytes: bytes, FlowKnown: true}
}
func newTrafficTestHistory(t *testing.T, options TrafficHistoryOptions, ifaces ...string) *TrafficHistory {
	t.Helper()
	h, err := NewTrafficHistory(10, ifaces, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Advance(at(0)); err != nil {
		t.Fatal(err)
	}
	return h
}
func TestTrafficWindows(t *testing.T) {
	h := newTrafficTestHistory(t, DefaultTrafficHistoryOptions(), "eth0")
	_ = h.SetInterfaceState("eth0", at(0), true)
	for _, p := range []metrics.TrafficPacket{trafficPacket(1, "192.0.2.2", 0, 17, 100), trafficPacket(.5, "192.0.2.10", 0, 6, 100), trafficPacket(2, "192.0.2.3", 0, 1, 100)} {
		if err := h.RecordPacket(p); err != nil {
			t.Fatal(err)
		}
	}
	_ = h.Advance(at(4))
	got, err := h.Snapshot(Window{at(.5), at(2)}, 20)
	if err != nil || got.Data == nil || got.CoverageRatio != 1 || got.Partial || !got.LastSampleAt.Equal(at(2)) {
		t.Fatalf("%+v %v", got, err)
	}
	if len(got.Data.Protocols) != 2 || got.Data.Protocols[0].Protocol != 6 || got.Data.Protocols[0].Bytes != 100 || got.Data.Protocols[0].Percent != 50 || len(got.Data.Flows) != 2 || got.Data.Flows[0].Flow.Source.Address.String() != "192.0.2.10" {
		t.Fatal(got.Data)
	}
	closeTo(t, got.Data.Flows[0].BytesPerSecond, 100/1.5)
	short, _ := h.Snapshot(Window{at(1), at(2)}, 1)
	if len(short.Data.Flows) != 1 || short.Partial || short.Data.Protocols[0].Protocol != 17 {
		t.Fatal(short)
	}
	quiet, _ := h.Snapshot(Window{at(3), at(4)}, 20)
	if quiet.Data == nil || len(quiet.Data.Protocols) != 0 || quiet.Partial {
		t.Fatal(quiet)
	}
	got.Data.Flows[0].BytesPerSecond = 999
	again, _ := h.Snapshot(Window{at(.5), at(2)}, 20)
	closeTo(t, again.Data.Flows[0].BytesPerSecond, 100/1.5)
}
func TestTrafficAvailability(t *testing.T) {
	h := newTrafficTestHistory(t, DefaultTrafficHistoryOptions(), "eth0", "eth1")
	_ = h.SetInterfaceState("eth0", at(1), true)
	_ = h.SetInterfaceState("eth0", at(3), false)
	_ = h.Advance(at(4))
	_ = h.RecordPacket(trafficPacket(2, "192.0.2.1", 0, 6, 100))
	got, err := h.Snapshot(Window{at(0), at(4)}, 20)
	if err != nil || got.Data == nil || got.CoverageRatio != .5 || !got.Partial {
		t.Fatalf("%+v %v", got, err)
	}
	closeTo(t, got.Data.Flows[0].BytesPerSecond, 50)
	absent, _ := h.Snapshot(Window{at(0), at(1)}, 20)
	if absent.Data != nil || absent.LastSampleAt != nil || absent.CoverageRatio != 0 {
		t.Fatal(absent)
	}
	h = newTrafficTestHistory(t, DefaultTrafficHistoryOptions(), "eth0", "eth1")
	_ = h.SetInterfaceState("eth0", at(0), true)
	_ = h.Advance(at(2))
	partial, _ := h.Snapshot(Window{at(0), at(2)}, 20)
	if partial.CoverageRatio != 1 || !partial.Partial {
		t.Fatal(partial)
	}
}
func TestTrafficLimitsAndLoss(t *testing.T) {
	h := newTrafficTestHistory(t, TrafficHistoryOptions{MaxPackets: 2, MaxFlows: 1}, "eth0")
	_ = h.SetInterfaceState("eth0", at(0), true)
	for i := 0; i < 3; i++ {
		if err := h.RecordPacket(trafficPacket(float64(i+1), "192.0.2.1", uint16(i), 6, 100)); err != nil {
			t.Fatal(err)
		}
	}
	_ = h.Advance(at(4))
	got, err := h.Snapshot(Window{at(0), at(4)}, 1)
	if err != nil || got.Data == nil || !got.Partial || got.Data.Protocols[0].Bytes != 200 {
		t.Fatalf("%+v %v", got, err)
	}
	if h.size > 2 || len(h.flows) > 1 {
		t.Fatal("unbounded storage")
	}
	_ = h.Advance(at(20))
	_ = h.RecordPacket(trafficPacket(20, "192.0.2.9", 1, 6, 100))
	_ = h.Advance(at(21))
	recent, _ := h.Snapshot(Window{at(20), at(21)}, 1)
	if recent.Partial || recent.Data == nil || len(h.flows) != 1 {
		t.Fatalf("expired losses persisted: %+v", recent)
	}
	_ = h.MarkLoss(at(20), at(21))
	recent, _ = h.Snapshot(Window{at(20), at(21)}, 1)
	if !recent.Partial {
		t.Fatal("lost loss marker")
	}
}
func TestTrafficValidation(t *testing.T) {
	for _, o := range []TrafficHistoryOptions{{}, {MaxPackets: 1, MaxFlows: 0}, {MaxPackets: 1 << 21, MaxFlows: 1}} {
		if _, err := NewTrafficHistory(10, []string{"eth0"}, o); err == nil {
			t.Fatal("invalid limits")
		}
	}
	if _, err := NewTrafficHistory(0, []string{"eth0"}, DefaultTrafficHistoryOptions()); err == nil {
		t.Fatal("invalid window")
	}
	if _, err := NewTrafficHistory(10, []string{"eth0", "eth0"}, DefaultTrafficHistoryOptions()); err == nil {
		t.Fatal("duplicates")
	}
	h := newTrafficTestHistory(t, DefaultTrafficHistoryOptions(), "eth0")
	if h.Advance(at(-1)) == nil || h.SetInterfaceState("unknown", at(0), true) == nil || h.MarkLoss(at(2), at(1)) == nil {
		t.Fatal("invalid timing")
	}
	_ = h.SetInterfaceState("eth0", at(1), true)
	if h.SetInterfaceState("eth0", at(0), false) == nil {
		t.Fatal("stale state")
	}
	p := trafficPacket(6, "192.0.2.1", 1, 6, 100)
	if h.RecordPacket(p) == nil {
		t.Fatal("future packet")
	}
	p.At = at(1)
	p.IPBytes = ^uint64(0)
	if h.RecordPacket(p) == nil {
		t.Fatal("oversized packet")
	}
	if _, err := h.Snapshot(Window{at(0), at(11)}, 20); err == nil {
		t.Fatal("server limit")
	}
	if _, err := h.Snapshot(Window{at(0), at(1)}, 0); err == nil {
		t.Fatal("topK")
	}
}
func TestTrafficConcurrentChurn(t *testing.T) {
	h := newTrafficTestHistory(t, TrafficHistoryOptions{MaxPackets: 32, MaxFlows: 4}, "eth0")
	_ = h.SetInterfaceState("eth0", at(0), true)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			now := float64(i) / 100
			_ = h.Advance(at(now))
			_ = h.RecordPacket(trafficPacket(now, "192.0.2.1", uint16(i), 6, 100))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_, _ = h.Snapshot(Window{at(0), at(10)}, 4)
		}
	}()
	wg.Wait()
	if h.size > 32 || len(h.flows) > 4 {
		t.Fatal("unbounded churn")
	}
	for i := 0; i < 100; i++ {
		_ = h.SetInterfaceState("eth0", at(10+float64(i)/100), i%2 == 0)
	}
	if len(h.interfaces["eth0"].closed) > h.maxStates {
		t.Fatal("unbounded states")
	}
}
func TestTrafficPortTie(t *testing.T) {
	h := newTrafficTestHistory(t, DefaultTrafficHistoryOptions(), "eth0")
	_ = h.SetInterfaceState("eth0", at(0), true)
	for _, port := range []uint16{1, 0} {
		_ = h.RecordPacket(trafficPacket(.5, "192.0.2.1", port, 6, 100))
	}
	_ = h.Advance(at(1))
	got, _ := h.Snapshot(Window{at(0), at(1)}, 20)
	if got.Data.Flows[0].Flow.Source.Port != 0 || !got.Data.Flows[0].Flow.Source.HasPort {
		t.Fatal(got)
	}
}

func TestTrafficFlowOrderingAndDirection(t *testing.T) {
	absent := metrics.FlowKey{Source: metrics.Endpoint{Address: netip.MustParseAddr("2001:db8::1")}, Destination: metrics.Endpoint{Address: netip.MustParseAddr("2001:db8::2")}, Protocol: 99}
	present := absent
	present.Source.HasPort = true
	if compareFlow(absent, present) >= 0 {
		t.Fatal("absent port must precede present port0")
	}
	second := absent
	second.Source.Address = netip.MustParseAddr("2001:db8::10")
	first := absent
	first.Source.Address = netip.MustParseAddr("2001:db8::2")
	if compareFlow(second, first) >= 0 {
		t.Fatal("IP ordering must compare strings")
	}
	first = absent
	second = absent
	second.Protocol = 100
	if compareFlow(first, second) >= 0 {
		t.Fatal("protocol tie ordering")
	}
	h := newTrafficTestHistory(t, DefaultTrafficHistoryOptions(), "eth0")
	_ = h.SetInterfaceState("eth0", at(0), true)
	packet := trafficPacket(.5, "192.0.2.1", 123, 17, 100)
	reverse := packet
	reverse.Flow.Source, reverse.Flow.Destination = packet.Flow.Destination, packet.Flow.Source
	if err := h.RecordPacket(packet); err != nil {
		t.Fatal(err)
	}
	if err := h.RecordPacket(reverse); err != nil {
		t.Fatal(err)
	}
	_ = h.Advance(at(1))
	result, err := h.Snapshot(Window{at(0), at(1)}, 20)
	if err != nil || result.Data == nil || len(result.Data.Flows) != 2 || result.Data.Protocols[0].Bytes != 200 {
		t.Fatalf("%+v %v", result, err)
	}
}
