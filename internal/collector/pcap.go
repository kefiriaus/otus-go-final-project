package collector

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const maxTrafficFrameBytes = 262144

type pcapFrame struct {
	At             time.Time
	LinkType       uint32
	OriginalLength uint32
	FCSBytes       uint32
	Data           []byte
}
type pcapReader struct {
	input      io.Reader
	order      binary.ByteOrder
	nano       bool
	link, snap uint32
	fcs        uint32
	buffer     []byte
}

func supportedTrafficLink(link uint32) bool {
	switch link {
	case 1, 101, 113, 276, 228, 229:
		return true
	}
	return false
}
func newPCAPReader(input io.Reader, maxFrameBytes int) (*pcapReader, error) {
	if maxFrameBytes < 1 || maxFrameBytes > maxTrafficFrameBytes {
		return nil, fmt.Errorf("invalid PCAP frame bound")
	}
	var h [24]byte
	if _, err := io.ReadFull(input, h[:]); err != nil {
		return nil, fmt.Errorf("PCAP global header: %w", err)
	}
	r := &pcapReader{input: input}
	switch [4]byte(h[:4]) {
	case [4]byte{0xd4, 0xc3, 0xb2, 0xa1}:
		r.order = binary.LittleEndian
	case [4]byte{0xa1, 0xb2, 0xc3, 0xd4}:
		r.order = binary.BigEndian
	case [4]byte{0x4d, 0x3c, 0xb2, 0xa1}:
		r.order = binary.LittleEndian
		r.nano = true
	case [4]byte{0xa1, 0xb2, 0x3c, 0x4d}:
		r.order = binary.BigEndian
		r.nano = true
	default:
		return nil, fmt.Errorf("unsupported PCAP magic")
	}
	if r.order.Uint16(h[4:]) != 2 || r.order.Uint16(h[6:]) != 4 {
		return nil, fmt.Errorf("unsupported PCAP version")
	}
	r.snap = r.order.Uint32(h[16:])
	word := r.order.Uint32(h[20:])
	r.link = word & 0xffff
	if r.snap == 0 || r.snap > uint32(maxFrameBytes) || word&0x0bff0000 != 0 || (word&0x04000000 == 0 && word&0xf0000000 != 0) || !supportedTrafficLink(r.link) {
		return nil, fmt.Errorf("invalid PCAP snaplen, link type, or flags")
	}
	if word&0x04000000 != 0 {
		r.fcs = (word >> 28) * 2
	}
	r.buffer = make([]byte, int(r.snap))
	return r, nil
}

// Next returns data backed by a reused buffer, valid only until the next call.
func (r *pcapReader) Next() (pcapFrame, error) {
	var h [16]byte
	n, err := io.ReadFull(r.input, h[:])
	if err != nil {
		if n == 0 && err == io.EOF {
			return pcapFrame{}, io.EOF
		}
		return pcapFrame{}, fmt.Errorf("PCAP record header: %w", err)
	}
	fraction, captured, original := r.order.Uint32(h[4:]), r.order.Uint32(h[8:]), r.order.Uint32(h[12:])
	limit := uint32(1000000)
	multiplier := int64(1000)
	if r.nano {
		limit = 1000000000
		multiplier = 1
	}
	if fraction >= limit || captured > r.snap || captured > original || original < r.fcs {
		return pcapFrame{}, fmt.Errorf("invalid PCAP record lengths or timestamp")
	}
	data := r.buffer[:int(captured)]
	if _, err := io.ReadFull(r.input, data); err != nil {
		return pcapFrame{}, fmt.Errorf("PCAP frame: %w", err)
	}
	return pcapFrame{At: time.Unix(int64(r.order.Uint32(h[:])), int64(fraction)*multiplier).UTC(), LinkType: r.link, OriginalLength: original, FCSBytes: r.fcs, Data: data}, nil
}
