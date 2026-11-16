package collector

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
)

func TestTrafficHistoryIntegration(t *testing.T) {
	history, err := aggregate.NewTrafficHistory(
		10,
		[]string{testCaptureInterface},
		aggregate.DefaultTrafficHistoryOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1000, 0).UTC()
	_ = history.Advance(start)
	_ = history.SetInterfaceState(testCaptureInterface, start, true)
	stream := pcapHeader(binary.LittleEndian, true, 101, maxTrafficFrameBytes)
	for i, protocol := range []byte{17, 6, 1} {
		data := ipv4Packet(protocol, 0, 53)
		if i == 1 {
			binary.BigEndian.PutUint16(data[6:], 1)
		}
		stream = append(
			stream,
			pcapRecord(binary.LittleEndian, 1000+uint32(i), 500000000, testUint32(len(data)), data)...)
	}
	reader, err := newPCAPReader(bytes.NewReader(stream), maxTrafficFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	for {
		frame, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		packet, ip, err := decodeTrafficFrame(frame, testCaptureInterface)
		if err != nil || !ip {
			t.Fatalf("%v", err)
		}
		if err := history.RecordPacket(packet); err != nil {
			t.Fatal(err)
		}
	}
	_ = history.Advance(start.Add(4 * time.Second))
	first, err := history.Snapshot(
		aggregate.Window{Start: start, End: start.Add(2 * time.Second)},
		20,
	)
	if err != nil || first.Data == nil || !first.Partial || first.CoverageRatio != 1 ||
		len(first.Data.Protocols) != 2 ||
		first.Data.Protocols[0].Protocol != 6 ||
		len(first.Data.Flows) != 1 ||
		first.Data.Flows[0].BytesPerSecond != 14 {
		t.Fatalf("%+v %v", first, err)
	}
	second, err := history.Snapshot(
		aggregate.Window{Start: start.Add(2 * time.Second), End: start.Add(4 * time.Second)},
		20,
	)
	if err != nil || second.Data == nil || second.Partial || len(second.Data.Protocols) != 1 ||
		second.Data.Protocols[0].Protocol != 1 ||
		second.Data.Protocols[0].Bytes != 28 {
		t.Fatalf("%+v %v", second, err)
	}
	again, _ := history.Snapshot(
		aggregate.Window{Start: start, End: start.Add(2 * time.Second)},
		20,
	)
	if len(again.Data.Protocols) != 2 || !again.Partial {
		t.Fatal("another window consumed history")
	}
}

func upLoopbackInterface(t *testing.T) string {
	t.Helper()
	all, err := net.Interfaces()
	if err != nil {
		t.Skipf("interface enumeration unavailable: %v", err)
	}
	for _, iface := range all {
		if iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagLoopback != 0 {
			return iface.Name
		}
	}
	t.Skip("no up loopback interface")
	return ""
}

func smokeUDPSockets(t *testing.T) (net.PacketConn, net.Conn, uint16) {
	t.Helper()
	var listenConfig net.ListenConfig
	receiver, err := listenConfig.ListenPacket(context.Background(), "udp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("local UDP unavailable: %v", err)
	}
	var dialer net.Dialer
	sender, err := dialer.DialContext(context.Background(), "udp4", receiver.LocalAddr().String())
	if err != nil {
		receiver.Close()
		t.Fatal(err)
	}
	return receiver, sender, testPort(receiver.LocalAddr().(*net.UDPAddr).Port)
}

func skipIfCapturePermissionDenied(
	t *testing.T,
	capture *TrafficCapture,
	cancel context.CancelFunc,
	done <-chan error,
) {
	t.Helper()
	failure := capture.LastError()
	if failure == nil {
		return
	}
	message := strings.ToLower(failure.Error())
	if strings.Contains(message, "permission") || strings.Contains(message, "permitted") {
		cancel()
		<-done
		t.Skipf("capture permission unavailable: %v", failure)
	}
}

func stopTrafficSmoke(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("smoke capture cleanup blocked")
	}
}

func TestTrafficLoopbackSmoke(t *testing.T) {
	if os.Getenv("OTUS_TRAFFIC_SMOKE") != "1" {
		t.Skip("set OTUS_TRAFFIC_SMOKE=1 for a bounded local UDP capture")
	}
	if _, err := exec.LookPath("tcpdump"); err != nil {
		t.Skipf("tcpdump unavailable: %v", err)
	}
	loopback := upLoopbackInterface(t)
	receiver, sender, target := smokeUDPSockets(t)
	defer receiver.Close()
	defer sender.Close()
	options := DefaultTrafficCaptureOptions()
	options.Interfaces = []string{loopback}
	options.IncludeLoopback = true
	capture, err := NewTrafficCapture(options)
	if err != nil {
		t.Fatal(err)
	}
	history, err := aggregate.NewTrafficHistory(
		5,
		capture.Interfaces(),
		aggregate.DefaultTrafficHistoryOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- capture.Run(ctx, history) }()
	start := time.Now().UTC()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	observed := false
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			_, _ = sender.Write([]byte{0x41})
			now := time.Now().UTC()
			snapshot, err := history.Snapshot(aggregate.Window{Start: start, End: now}, 20)
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			if snapshot.Data != nil {
				for _, flow := range snapshot.Data.Flows {
					if flow.Flow.Protocol == 17 && flow.Flow.Destination.HasPort && flow.Flow.Destination.Port == target {
						observed = true
						break loop
					}
				}
			}
			skipIfCapturePermissionDenied(t, capture, cancel, done)
		}
	}
	stopTrafficSmoke(t, cancel, done)
	if !observed {
		t.Fatalf("synthetic UDP was not observed: %v", capture.LastError())
	}
	t.Log("captured synthetic loopback UDP metadata; no payload retained")
}
