package collector

import (
	"encoding/binary"
	"net/netip"
	"testing"
	"time"
)

func ipv4Packet(protocol byte, srcPort, dstPort uint16) []byte {
	b := make([]byte, 28)
	b[0] = 0x45
	binary.BigEndian.PutUint16(b[2:], 28)
	b[9] = protocol
	copy(b[12:], []byte{192, 0, 2, 1, 198, 51, 100, 2})
	binary.BigEndian.PutUint16(b[20:], srcPort)
	binary.BigEndian.PutUint16(b[22:], dstPort)
	return b
}

func ipv6Packet(protocol byte) []byte {
	b := make([]byte, 48)
	b[0] = 0x60
	binary.BigEndian.PutUint16(b[4:], 8)
	b[6] = protocol
	source := netip.MustParseAddr("2001:db8::1").As16()
	dest := netip.MustParseAddr("2001:db8::2").As16()
	copy(b[8:], source[:])
	copy(b[24:], dest[:])
	binary.BigEndian.PutUint16(b[40:], 123)
	binary.BigEndian.PutUint16(b[42:], 456)
	return b
}

func trafficFrame(link uint32, data []byte) pcapFrame {
	return pcapFrame{
		At:             time.Unix(1000, 0).UTC(),
		LinkType:       link,
		OriginalLength: testUint32(len(data)),
		Data:           data,
	}
}

func testTrafficIPProtocols(t *testing.T) {
	t.Helper()
	for _, protocol := range []byte{6, 17, 1, 99} {
		packet, ok, err := decodeTrafficFrame(trafficFrame(101, ipv4Packet(protocol, 0, 53)), "eth0")
		if err != nil || !ok || packet.IPBytes != 28 || packet.Flow.Protocol != protocol ||
			!packet.FlowKnown || packet.Flow.Source.Address.String() != "192.0.2.1" {
			t.Fatalf("%+v %v", packet, err)
		}
		if packet.Flow.Source.HasPort != (protocol == 6 || protocol == 17) ||
			packet.Flow.Source.Port != 0 {
			t.Fatal(packet)
		}
	}
	for _, protocol := range []byte{6, 17, 58, 50, 253} {
		packet, ok, err := decodeTrafficFrame(trafficFrame(101, ipv6Packet(protocol)), "eth0")
		if err != nil || !ok || packet.IPBytes != 48 || packet.Flow.Protocol != protocol ||
			packet.Flow.Destination.Address.String() != "2001:db8::2" ||
			packet.Flow.Source.HasPort != (protocol == 6 || protocol == 17) {
			t.Fatalf("%+v %v", packet, err)
		}
	}
}

func testTrafficLinkTypes(t *testing.T) {
	t.Helper()
	for _, link := range []uint32{1, 113, 276, 228, 229} {
		ip := ipv4Packet(17, 1, 2)
		proto := uint16(0x800)
		if link == 229 {
			ip = ipv6Packet(17)
			proto = 0x86dd
		}
		h := make([]byte, 0, 20)
		switch link {
		case 1:
			h = h[:14]
			binary.BigEndian.PutUint16(h[12:], proto)
		case 113:
			h = h[:16]
			binary.BigEndian.PutUint16(h[14:], proto)
		case 276:
			h = h[:20]
			binary.BigEndian.PutUint16(h, proto)
		}
		h = append(h, ip...)
		packet, ok, err := decodeTrafficFrame(trafficFrame(link, h), "eth0")
		if err != nil || !ok || packet.IPBytes != uint64(len(ip)) {
			t.Fatalf("link %d: %+v %v", link, packet, err)
		}
	}
}

func testTrafficVLANAndNonIP(t *testing.T) {
	t.Helper()
	eth := make([]byte, 22, 70)
	binary.BigEndian.PutUint16(eth[12:], 0x88a8)
	binary.BigEndian.PutUint16(eth[16:], 0x8100)
	binary.BigEndian.PutUint16(eth[20:], 0x800)
	eth = append(eth, ipv4Packet(17, 1, 2)...)
	eth = append(eth, make([]byte, 20)...)
	packet, ok, err := decodeTrafficFrame(trafficFrame(1, eth), "eth0")
	if err != nil || !ok || packet.IPBytes != 28 {
		t.Fatalf("%+v %v", packet, err)
	}
	nonIP := make([]byte, 14)
	binary.BigEndian.PutUint16(nonIP[12:], 0x806)
	if _, ok, err := decodeTrafficFrame(trafficFrame(1, nonIP), "eth0"); err != nil || ok {
		t.Fatal("ARP counted")
	}
}

func TestTrafficPacket(t *testing.T) {
	testTrafficIPProtocols(t)
	testTrafficLinkTypes(t)
	testTrafficVLANAndNonIP(t)
}

func TestTrafficPacketFragmentsAndTruncation(t *testing.T) {
	for _, bits := range []uint16{0x2000, 1} {
		data := ipv4Packet(6, 1, 2)
		binary.BigEndian.PutUint16(data[6:], bits)
		packet, _, err := decodeTrafficFrame(trafficFrame(101, data), "eth0")
		if err != nil || !packet.Partial || packet.FlowKnown != (bits == 0x2000) {
			t.Fatalf("%+v %v", packet, err)
		}
	}
	full := ipv4Packet(17, 1, 2)
	frame := trafficFrame(101, full)
	frame.Data = full[:22]
	packet, _, err := decodeTrafficFrame(frame, "eth0")
	if err != nil || packet.FlowKnown || !packet.Partial || packet.IPBytes != 28 {
		t.Fatalf("%+v %v", packet, err)
	}
	data := ipv6Packet(0)
	data = append(data, make([]byte, 8)...)
	binary.BigEndian.PutUint16(data[4:], 16)
	data[40] = 17
	data[41] = 0
	binary.BigEndian.PutUint16(data[48:], 0)
	binary.BigEndian.PutUint16(data[50:], 53)
	packet, _, err = decodeTrafficFrame(trafficFrame(101, data), "eth0")
	if err != nil || packet.Flow.Protocol != 17 || !packet.Flow.Source.HasPort ||
		packet.Flow.Source.Port != 0 {
		t.Fatalf("%+v %v", packet, err)
	}
	data[6] = 44
	data[40] = 6
	binary.BigEndian.PutUint16(data[42:], 8)
	packet, _, err = decodeTrafficFrame(trafficFrame(101, data), "eth0")
	if err != nil || !packet.Partial || packet.FlowKnown || packet.Flow.Protocol != 6 {
		t.Fatalf("%+v %v", packet, err)
	}
}

func TestTrafficPacketInvalid(t *testing.T) {
	for _, mode := range []string{
		testShortMode, testVersionMode, "ihl", "total", "wire", "extension", "jumbo", testLinkMode,
	} {
		frame := trafficFrame(101, ipv4Packet(17, 1, 2))
		switch mode {
		case testShortMode:
			frame.Data = frame.Data[:10]
		case testVersionMode:
			frame.Data[0] = 0x75
		case "ihl":
			frame.Data[0] = 0x44
		case "total":
			binary.BigEndian.PutUint16(frame.Data[2:], 19)
		case "wire":
			frame.OriginalLength = 20
		case "extension":
			frame = trafficFrame(101, ipv6Packet(0))
			frame.Data[41] = 255
		case "jumbo":
			frame = trafficFrame(101, ipv6Packet(0))
			binary.BigEndian.PutUint16(frame.Data[4:], 0)
		case testLinkMode:
			frame.LinkType = 999
		}
		if _, _, err := decodeTrafficFrame(frame, "eth0"); err == nil {
			t.Fatalf("accepted %s", mode)
		}
	}
}

func FuzzTrafficPacket(f *testing.F) {
	f.Add(ipv4Packet(17, 1, 2))
	f.Add(ipv6Packet(6))
	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > maxTrafficFrameBytes {
			return
		}
		_, _, _ = decodeTrafficFrame(trafficFrame(101, data), "eth0")
	})
}

func TestTrafficPacketAHLength(t *testing.T) {
	invalid := ipv6Packet(51)
	invalid = append(invalid, make([]byte, 8)...)
	binary.BigEndian.PutUint16(invalid[4:], 16)
	invalid[40] = 17
	invalid[41] = 0
	if _, _, err := decodeTrafficFrame(trafficFrame(101, invalid), "eth0"); err == nil {
		t.Fatal("undersized AH accepted")
	}
	for _, extension := range []byte{43, 60, 51} {
		size := 8
		if extension == 51 {
			size = 12
		}
		data := ipv6Packet(extension)
		data = append(data, make([]byte, size)...)
		binary.BigEndian.PutUint16(data[4:], uint16(8+size))
		data[40] = 17
		data[41] = 0
		if extension == 51 {
			data[41] = 1
		}
		binary.BigEndian.PutUint16(data[40+size:], 123)
		binary.BigEndian.PutUint16(data[42+size:], 456)
		packet, ok, err := decodeTrafficFrame(trafficFrame(101, data), "eth0")
		if err != nil || !ok || packet.Flow.Protocol != 17 || packet.Flow.Source.Port != 123 ||
			packet.Flow.Destination.Port != 456 {
			t.Fatalf("extension %d: %+v %v", extension, packet, err)
		}
	}
}
