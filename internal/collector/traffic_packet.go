package collector

import (
	"encoding/binary"
	"fmt"
	"net/netip"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func ipv6Extension(p byte) bool {
	switch p {
	case 0, 43, 44, 51, 60:
		return true
	}
	return false
}

func malformedTrafficFrame() error {
	return fmt.Errorf("malformed or unsupported traffic frame")
}

func decodeLinkFrame(frame pcapFrame) ([]byte, uint16, uint64, bool, error) {
	data := frame.Data
	offset := 0
	var ether uint16
	if uint64(len(data)) > uint64(frame.OriginalLength) || len(data) > maxTrafficFrameBytes {
		return nil, 0, 0, false, malformedTrafficFrame()
	}
	switch frame.LinkType {
	case 1:
		if len(data) < 14 {
			return nil, 0, 0, false, malformedTrafficFrame()
		}
		offset = 14
		ether = binary.BigEndian.Uint16(data[12:])
		for tags := 0; ether == 0x8100 || ether == 0x88a8 || ether == 0x9100; tags++ {
			if tags >= 4 || len(data) < offset+4 {
				return nil, 0, 0, false, malformedTrafficFrame()
			}
			ether = binary.BigEndian.Uint16(data[offset+2:])
			offset += 4
		}
	case 113:
		if len(data) < 16 {
			return nil, 0, 0, false, malformedTrafficFrame()
		}
		offset = 16
		ether = binary.BigEndian.Uint16(data[14:])
	case 276:
		if len(data) < 20 {
			return nil, 0, 0, false, malformedTrafficFrame()
		}
		offset = 20
		ether = binary.BigEndian.Uint16(data)
	case 101:
		if len(data) == 0 {
			return nil, 0, 0, false, malformedTrafficFrame()
		}
		switch data[0] >> 4 {
		case 4:
			ether = 0x800
		case 6:
			ether = 0x86dd
		default:
			return nil, 0, 0, false, malformedTrafficFrame()
		}
	case 228:
		ether = 0x800
	case 229:
		ether = 0x86dd
	default:
		return nil, 0, 0, false, malformedTrafficFrame()
	}
	if ether != 0x800 && ether != 0x86dd {
		return nil, ether, 0, false, nil
	}
	if uint64(frame.OriginalLength) < uint64(offset)+uint64(frame.FCSBytes) {
		return nil, 0, 0, false, malformedTrafficFrame()
	}
	wire := uint64(frame.OriginalLength) - uint64(offset) - uint64(frame.FCSBytes)
	return data[offset:], ether, wire, true, nil
}

func decodeIPv4(ip []byte, wire uint64, packet *metrics.TrafficPacket) (int, bool, error) {
	if len(ip) < 20 || ip[0]>>4 != 4 {
		return 0, false, malformedTrafficFrame()
	}
	header := int(ip[0]&15) * 4
	total := int(binary.BigEndian.Uint16(ip[2:]))
	// #nosec G115 -- The IPv4 total-length field is a nonnegative uint16.
	if header < 20 || header > len(ip) || total < header || uint64(total) > wire {
		return 0, false, malformedTrafficFrame()
	}
	// #nosec G115 -- total is nonnegative and bounded by the parsed IP length.
	packet.IPBytes = uint64(total)
	packet.Flow.Protocol = ip[9]
	packet.Flow.Source.Address = netip.AddrFrom4([4]byte(ip[12:16]))
	packet.Flow.Destination.Address = netip.AddrFrom4([4]byte(ip[16:20]))
	fragment := binary.BigEndian.Uint16(ip[6:])
	noninitial := fragment&0x1fff != 0
	packet.Partial = noninitial || fragment&0x2000 != 0
	return header, noninitial, nil
}

func ipv6ExtensionSize(ip []byte, transport int, protocol byte, packet *metrics.TrafficPacket) (int, bool, error) {
	switch protocol {
	case 44:
		if len(ip) < transport+8 {
			return 0, false, malformedTrafficFrame()
		}
		fragment := binary.BigEndian.Uint16(ip[transport+2:])
		noninitial := fragment&0xfff8 != 0
		packet.Partial = packet.Partial || noninitial || fragment&1 != 0
		return 8, noninitial, nil
	case 51:
		size := (int(ip[transport+1]) + 2) * 4
		if size < 12 {
			return 0, false, malformedTrafficFrame()
		}
		return size, false, nil
	default:
		return (int(ip[transport+1]) + 1) * 8, false, nil
	}
}

func decodeIPv6(ip []byte, wire uint64, packet *metrics.TrafficPacket) (int, bool, error) {
	if len(ip) < 40 || ip[0]>>4 != 6 {
		return 0, false, malformedTrafficFrame()
	}
	payload := int(binary.BigEndian.Uint16(ip[4:]))
	if payload == 0 && ip[6] == 0 {
		return 0, false, malformedTrafficFrame()
	}
	total := 40 + payload
	if uint64(total) > wire {
		return 0, false, malformedTrafficFrame()
	}
	ip = ip[:min(len(ip), total)]
	// #nosec G115 -- total is nonnegative and bounded by the parsed IP length.
	packet.IPBytes = uint64(total)
	packet.Flow.Source.Address = netip.AddrFrom16([16]byte(ip[8:24]))
	packet.Flow.Destination.Address = netip.AddrFrom16([16]byte(ip[24:40]))
	transport := 40
	protocol := ip[6]
	noninitial := false
	for depth := 0; ipv6Extension(protocol); depth++ {
		if depth >= 16 || len(ip) < transport+2 {
			return 0, false, malformedTrafficFrame()
		}
		next := ip[transport]
		size, fragment, err := ipv6ExtensionSize(ip, transport, protocol, packet)
		if err != nil {
			return 0, false, err
		}
		noninitial = noninitial || fragment
		if size < 8 || size > len(ip)-transport {
			return 0, false, malformedTrafficFrame()
		}
		transport += size
		protocol = next
		if noninitial {
			if ipv6Extension(protocol) {
				return 0, false, malformedTrafficFrame()
			}
			break
		}
	}
	packet.Flow.Protocol = protocol
	return transport, noninitial, nil
}

func decodeTrafficFrame(frame pcapFrame, iface string) (metrics.TrafficPacket, bool, error) {
	ip, ether, wire, ok, err := decodeLinkFrame(frame)
	if err != nil || !ok {
		return metrics.TrafficPacket{}, false, err
	}
	packet := metrics.TrafficPacket{At: frame.At.UTC(), Interface: iface, FlowKnown: true}
	var transport int
	var noninitial bool
	if ether == 0x800 {
		transport, noninitial, err = decodeIPv4(ip, wire, &packet)
	} else {
		transport, noninitial, err = decodeIPv6(ip, wire, &packet)
	}
	if err != nil {
		return metrics.TrafficPacket{}, false, err
	}
	// #nosec G115 -- IPBytes was parsed from bounded IPv4 or IPv6 lengths.
	total := int(packet.IPBytes)
	ip = ip[:min(len(ip), total)]
	if packet.Flow.Protocol == 6 || packet.Flow.Protocol == 17 {
		if noninitial || len(ip) < transport+4 {
			packet.FlowKnown = false
			packet.Partial = true
		} else {
			packet.Flow.Source.HasPort = true
			packet.Flow.Destination.HasPort = true
			packet.Flow.Source.Port = binary.BigEndian.Uint16(ip[transport:])
			packet.Flow.Destination.Port = binary.BigEndian.Uint16(ip[transport+2:])
		}
	}
	return packet, true, nil
}
