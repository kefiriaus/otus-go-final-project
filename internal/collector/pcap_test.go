package collector

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"
)

const (
	testLinkMode    = "link"
	testShortMode   = "short"
	testVersionMode = "version"
)

func testUint32(value int) uint32 {
	if value < 0 || uint64(value) > uint64(^uint32(0)) {
		panic("test value exceeds uint32")
	}
	// #nosec G115 -- The range is checked immediately above.
	return uint32(value)
}

func pcapHeader(order binary.ByteOrder, nano bool, link, snap uint32) []byte {
	b := make([]byte, 24)
	magic := uint32(0xa1b2c3d4)
	if nano {
		magic = 0xa1b23c4d
	}
	order.PutUint32(b, magic)
	order.PutUint16(b[4:], 2)
	order.PutUint16(b[6:], 4)
	order.PutUint32(b[16:], snap)
	order.PutUint32(b[20:], link)
	return b
}

func pcapRecord(order binary.ByteOrder, sec, fraction, original uint32, data []byte) []byte {
	b := make([]byte, 16, 16+len(data))
	order.PutUint32(b, sec)
	order.PutUint32(b[4:], fraction)
	// #nosec G115 -- Test packets are far below the uint32 size limit.
	order.PutUint32(b[8:], testUint32(len(data)))
	order.PutUint32(b[12:], original)
	b = append(b, data...)
	return b
}

func TestPCAP(t *testing.T) {
	for _, order := range []binary.ByteOrder{binary.LittleEndian, binary.BigEndian} {
		for _, nano := range []bool{false, true} {
			header := pcapHeader(order, nano, 101, 1024)
			order.PutUint32(header[8:], 123)
			order.PutUint32(header[12:], 456)
			header = append(header, pcapRecord(order, 1000, 123, 4, []byte{1, 2, 3, 4})...)
			reader, err := newPCAPReader(bytes.NewReader(header), 1024)
			if err != nil {
				t.Fatal(err)
			}
			frame, err := reader.Next()
			want := int64(123000)
			if nano {
				want = 123
			}
			if err != nil || frame.LinkType != 101 || frame.OriginalLength != 4 ||
				!bytes.Equal(frame.Data, []byte{1, 2, 3, 4}) ||
				!frame.At.Equal(time.Unix(1000, want).UTC()) {
				t.Fatalf("%+v %v", frame, err)
			}
			if _, err := reader.Next(); !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
		}
	}
}

func TestPCAPInvalid(t *testing.T) {
	for _, mode := range []string{"magic", testVersionMode, "snap", testLinkMode, "flags", "fcs", testShortMode} {
		header := pcapHeader(binary.LittleEndian, false, 101, 1024)
		switch mode {
		case "magic":
			header[0] = 0
		case testVersionMode:
			header[4] = 3
		case "snap":
			binary.LittleEndian.PutUint32(header[16:], 1025)
		case testLinkMode:
			binary.LittleEndian.PutUint32(header[20:], 999)
		case "flags":
			binary.LittleEndian.PutUint32(header[20:], 0x10000|101)
		case "fcs":
			binary.LittleEndian.PutUint32(header[20:], 0x10000000|101)
		case testShortMode:
			header = header[:23]
		}
		if _, err := newPCAPReader(bytes.NewReader(header), 1024); err == nil {
			t.Fatalf("accepted %s", mode)
		}
	}
	for _, mode := range []string{"fraction", "captured", "original", "short header", "short body"} {
		header := pcapHeader(binary.LittleEndian, false, 101, 1024)
		record := pcapRecord(binary.LittleEndian, 1000, 0, 4, []byte{1, 2, 3, 4})
		switch mode {
		case "fraction":
			binary.LittleEndian.PutUint32(record[4:], 1000000)
		case "captured":
			binary.LittleEndian.PutUint32(record[8:], 0xffffffff)
		case "original":
			binary.LittleEndian.PutUint32(record[12:], 3)
		case "short header":
			record = record[:15]
		case "short body":
			record = record[:18]
		}
		reader, err := newPCAPReader(bytes.NewReader(append(header, record...)), 1024)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.Next(); err == nil || errors.Is(err, io.EOF) {
			t.Fatalf("accepted %s: %v", mode, err)
		}
	}
	header := pcapHeader(binary.LittleEndian, false, 1|0x24000000, 1024)
	if _, err := newPCAPReader(bytes.NewReader(header), 1024); err != nil {
		t.Fatalf("valid FCS flags: %v", err)
	}
}

func FuzzPCAP(f *testing.F) {
	f.Add(pcapHeader(binary.LittleEndian, false, 101, 1024))
	f.Add([]byte("bad"))
	f.Fuzz(func(_ *testing.T, b []byte) {
		reader, err := newPCAPReader(bytes.NewReader(b), 4096)
		if err == nil {
			for i := 0; i < 8; i++ {
				if _, err := reader.Next(); err != nil {
					break
				}
			}
		}
	})
}

func TestPCAPFCSWireLength(t *testing.T) {
	header := pcapHeader(binary.LittleEndian, false, 1|0x24000000, 1024)
	ethernet := make([]byte, 14, 46)
	binary.BigEndian.PutUint16(ethernet[12:], 0x800)
	ethernet = append(ethernet, ipv4Packet(17, 1, 2)...)
	ethernet = append(ethernet, make([]byte, 4)...)
	data := ethernet
	// #nosec G115 -- The fixture is intentionally much smaller than uint32 capacity.
	reader, err := newPCAPReader(
		bytes.NewReader(
			append(header, pcapRecord(binary.LittleEndian, 1000, 0, testUint32(len(data)), data)...),
		),
		1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	packet, _, err := decodeTrafficFrame(frame, "eth0")
	if err != nil || packet.IPBytes != 28 {
		t.Fatalf("%+v %v", packet, err)
	}
	binary.BigEndian.PutUint16(frame.Data[16:], 32)
	if _, _, err := decodeTrafficFrame(frame, "eth0"); err == nil {
		t.Fatal("FCS bytes accepted as IP payload")
	}
}
