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
func decodeTrafficFrame(frame pcapFrame, iface string) (metrics.TrafficPacket, bool, error) {
	fail := func() (metrics.TrafficPacket, bool, error) {
		return metrics.TrafficPacket{}, false, fmt.Errorf("malformed or unsupported traffic frame")
	}
	data := frame.Data
	offset := 0
	var ether uint16
	if uint64(len(data)) > uint64(frame.OriginalLength) || len(data) > maxTrafficFrameBytes {
		return fail()
	}
	switch frame.LinkType {
	case 1:
		if len(data) < 14 {
			return fail()
		}
		offset = 14
		ether = binary.BigEndian.Uint16(data[12:])
		for tags := 0; ether == 0x8100 || ether == 0x88a8 || ether == 0x9100; tags++ {
			if tags >= 4 || len(data) < offset+4 {
				return fail()
			}
			ether = binary.BigEndian.Uint16(data[offset+2:])
			offset += 4
		}
	case 113:
		if len(data) < 16 {
			return fail()
		}
		offset = 16
		ether = binary.BigEndian.Uint16(data[14:])
	case 276:
		if len(data) < 20 {
			return fail()
		}
		offset = 20
		ether = binary.BigEndian.Uint16(data)
	case 101:
		if len(data) == 0 {
			return fail()
		}
		switch data[0] >> 4 {
		case 4:
			ether = 0x800
		case 6:
			ether = 0x86dd
		default:
			return fail()
		}
	case 228:
		ether = 0x800
	case 229:
		ether = 0x86dd
	default:
		return fail()
	}
	if ether != 0x800 && ether != 0x86dd {
		return metrics.TrafficPacket{}, false, nil
	}
	ip := data[offset:]
	if uint64(frame.OriginalLength) < uint64(offset)+uint64(frame.FCSBytes) {
		return fail()
	}
	wire := uint64(frame.OriginalLength) - uint64(offset) - uint64(frame.FCSBytes)
	packet := metrics.TrafficPacket{At: frame.At.UTC(), Interface: iface, FlowKnown: true}
	transport := 0
	noninitial := false
	if ether == 0x800 {
		if len(ip) < 20 || ip[0]>>4 != 4 {
			return fail()
		}
		header := int(ip[0]&15) * 4
		total := int(binary.BigEndian.Uint16(ip[2:]))
		if header < 20 || header > len(ip) || total < header || uint64(total) > wire {
			return fail()
		}
		ip = ip[:min(len(ip), total)]
		packet.IPBytes = uint64(total)
		packet.Flow.Protocol = ip[9]
		packet.Flow.Source.Address = netip.AddrFrom4([4]byte(ip[12:16]))
		packet.Flow.Destination.Address = netip.AddrFrom4([4]byte(ip[16:20]))
		transport = header
		fragment := binary.BigEndian.Uint16(ip[6:])
		noninitial = fragment&0x1fff != 0
		packet.Partial = noninitial || fragment&0x2000 != 0
	} else {
		if len(ip) < 40 || ip[0]>>4 != 6 {
			return fail()
		}
		payload := int(binary.BigEndian.Uint16(ip[4:]))
		if payload == 0 && ip[6] == 0 {
			return fail()
		}
		total := 40 + payload
		if uint64(total) > wire {
			return fail()
		}
		ip = ip[:min(len(ip), total)]
		packet.IPBytes = uint64(total)
		packet.Flow.Source.Address = netip.AddrFrom16([16]byte(ip[8:24]))
		packet.Flow.Destination.Address = netip.AddrFrom16([16]byte(ip[24:40]))
		transport = 40
		protocol := ip[6]
		for depth := 0; ipv6Extension(protocol); depth++ {
			if depth >= 16 || len(ip) < transport+2 {
				return fail()
			}
			next := ip[transport]
			size := 0
			switch protocol {
			case 44:
				if len(ip) < transport+8 {
					return fail()
				}
				fragment := binary.BigEndian.Uint16(ip[transport+2:])
				noninitial = fragment&0xfff8 != 0
				packet.Partial = packet.Partial || noninitial || fragment&1 != 0
				size = 8
			case 51:
				size = (int(ip[transport+1]) + 2) * 4
				if size < 12 {
					return fail()
				}
			default:
				size = (int(ip[transport+1]) + 1) * 8
			}
			if size < 8 || size > len(ip)-transport {
				return fail()
			}
			transport += size
			protocol = next
			if noninitial {
				if ipv6Extension(protocol) {
					return fail()
				}
				break
			}
		}
		packet.Flow.Protocol = protocol
	}
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
