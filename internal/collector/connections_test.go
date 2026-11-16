package collector

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
)

const socketTableHeader = "sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"

func socketLine(slot int, local, remote, state string, inode uint64) string {
	return fmt.Sprintf(
		"%d: %s %s %s 00000000:00000000 00:00000000 00000000 999 0 %d\n",
		slot,
		local,
		remote,
		state,
		inode,
	)
}

func TestParseSocketTable(t *testing.T) {
	for _, tc := range []struct {
		local, remote string
		ipv6          bool
		order         binary.ByteOrder
		want          string
	}{
		{"0100007F:1F90", "00000000:0000", false, binary.LittleEndian, testIPv4Loopback},
		{"7F000001:0000", "00000000:0000", false, binary.BigEndian, testIPv4Loopback},
		{
			"00000000000000000000000001000000:FFFF", testZeroIPv6Endpoint,
			true, binary.LittleEndian, testIPv6Loopback,
		},
		{
			"00000000000000000000000000000001:FFFF", testZeroIPv6Endpoint,
			true, binary.BigEndian, testIPv6Loopback,
		},
		{
			"0000000000000000FFFF00000100007F:1234", testZeroIPv6Endpoint,
			true, binary.LittleEndian, "::ffff:127.0.0.1",
		},
	} {
		got, err := parseSocketTable(
			[]byte(socketTableHeader+socketLine(0, tc.local, tc.remote, "0A", 123)),
			6,
			tc.ipv6,
			tc.order,
			10,
		)
		if err != nil || len(got) != 1 || got[0].Local.Address.String() != tc.want ||
			!got[0].Local.HasPort ||
			got[0].Inode != 123 ||
			got[0].StateCode != 10 {
			t.Fatalf("%+v %v", got, err)
		}
	}
	empty, err := parseSocketTable([]byte(socketTableHeader), 17, false, binary.LittleEndian, 10)
	if err != nil || len(empty) != 0 {
		t.Fatalf("%+v %v", empty, err)
	}
	short := socketTableHeader + "0: 00000000:0000 00000000:0000 06\n"
	got, err := parseSocketTable([]byte(short), 6, false, binary.LittleEndian, 10)
	if err != nil || len(got) != 1 || got[0].Inode != 0 ||
		tcpStateName(got[0].StateCode) != "TIME_WAIT" {
		t.Fatalf("%+v %v", got, err)
	}
	stateNames := map[uint8]string{
		1: "ESTABLISHED", 2: "SYN_SENT", 3: "SYN_RECV", 4: "FIN_WAIT1",
		5: "FIN_WAIT2", 6: "TIME_WAIT", 7: "CLOSE", 8: "CLOSE_WAIT",
		9: "LAST_ACK", 10: "LISTEN", 11: "CLOSING", 12: "NEW_SYN_RECV",
		13: "BOUND_INACTIVE", 255: "UNKNOWN_FF",
	}
	for code, want := range stateNames {
		if got := tcpStateName(code); got != want {
			t.Fatalf("%d: %s", code, got)
		}
	}
}

func TestParseSocketTableInvalid(t *testing.T) {
	valid := socketTableHeader + socketLine(0, "0100007F:1F90", "00000000:0000", "0A", 123)
	inputs := []string{
		"", "bad header\n",
		strings.Replace(valid, "0100007F", "bad", 1),
		strings.Replace(valid, "1F90", "10000", 1),
		strings.Replace(valid, "0A", "GG", 1),
		strings.Replace(valid, "123", "18446744073709551616", 1),
		socketTableHeader + "bad row\n",
		socketTableHeader + strings.Repeat("x", 4097),
	}
	for _, input := range inputs {
		if _, err := parseSocketTable(
			[]byte(input),
			6,
			false,
			binary.LittleEndian,
			10,
		); err == nil {
			t.Fatalf("accepted %q", input[:min(len(input), 80)])
		}
	}
	if _, err := parseSocketTable(
		[]byte(valid+strings.TrimPrefix(valid, socketTableHeader)),
		6,
		false,
		binary.LittleEndian,
		1,
	); err == nil {
		t.Fatal("row cap")
	}
	if _, err := parseSocketTable(
		make([]byte, (4<<20)+1),
		6,
		false,
		binary.LittleEndian,
		10,
	); err == nil {
		t.Fatal("file cap")
	}
}

func TestConnectionMetadataParsers(t *testing.T) {
	for _, tc := range []struct {
		value  string
		inode  uint64
		socket bool
	}{
		{"socket:[123]", 123, true},
		{"pipe:[123]", 0, false},
		{"anon_inode:[eventpoll]", 0, false},
		{"/tmp/file", 0, false},
	} {
		inode, socket, err := parseSocketInode(tc.value)
		if err != nil || inode != tc.inode || socket != tc.socket {
			t.Fatalf("%d %v %v", inode, socket, err)
		}
	}
	for _, bad := range []string{"socket:[0]", "socket:[bad]", "socket:[-1]", "socket:[1]extra", "socket:bad"} {
		if _, _, err := parseSocketInode(bad); err == nil {
			t.Fatal(bad)
		}
	}
	command, err := parseProcessComm([]byte(" name\ninside \n"))
	if err != nil || command != " name\ninside " {
		t.Fatalf("%q %v", command, err)
	}
	if _, err := parseProcessComm([]byte{255}); err == nil {
		t.Fatal("invalid UTF8")
	}
	uid, err := parseEffectiveUID([]byte("Name:\ttest\nUid:\t1000 2000 3000 4000\n"))
	if err != nil || uid != "2000" {
		t.Fatalf("%q %v", uid, err)
	}
	for _, bad := range []string{"", "Uid: 1 2", "Uid: 1 bad 3 4", "Uid: 1 4294967296 3 4"} {
		if _, err := parseEffectiveUID([]byte(bad)); err == nil {
			t.Fatal(bad)
		}
	}
}

const testZeroIPv6Endpoint = "00000000000000000000000000000000:0000"

func FuzzSocketTable(f *testing.F) {
	f.Add(
		[]byte(socketTableHeader+socketLine(0, "0100007F:1F90", "00000000:0000", "0A", 123)),
		false,
	)
	f.Add([]byte("bad"), true)
	f.Fuzz(func(_ *testing.T, data []byte, ipv6 bool) {
		if len(data) > 65536 {
			return
		}
		_, _ = parseSocketTable(data, 6, ipv6, binary.LittleEndian, 128)
	})
}
