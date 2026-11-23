package collector

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

const maxSocketTableBytes = 4 << 20

var errSocketRowLimit = errors.New("socket row limit exceeded")

type socketRow struct {
	Local, Remote       metrics.Endpoint
	Protocol, StateCode uint8
	Inode               uint64
}

func parseSocketAddress(value string, ipv6 bool, order binary.ByteOrder) (metrics.Endpoint, error) {
	length := 8
	if ipv6 {
		length = 32
	}
	if len(value) != length+5 || value[length] != ':' {
		return metrics.Endpoint{}, fmt.Errorf("invalid socket endpoint width")
	}
	var raw [16]byte
	for offset := 0; offset < length; offset += 8 {
		word, err := strconv.ParseUint(value[offset:offset+8], 16, 32)
		if err != nil {
			return metrics.Endpoint{}, fmt.Errorf("socket address: %w", err)
		}
		order.PutUint32(raw[offset/2:], uint32(word))
	}
	port, err := strconv.ParseUint(value[length+1:], 16, 16)
	if err != nil {
		return metrics.Endpoint{}, fmt.Errorf("socket port: %w", err)
	}
	address := netip.AddrFrom4([4]byte(raw[:4]))
	if ipv6 {
		address = netip.AddrFrom16(raw)
	}
	return metrics.Endpoint{Address: address, Port: uint16(port), HasPort: true}, nil
}

func parseSocketRow(
	fields []string,
	protocol uint8,
	ipv6 bool,
	order binary.ByteOrder,
) (socketRow, error) {
	if len(fields) < 4 || !strings.HasSuffix(fields[0], ":") {
		return socketRow{}, fmt.Errorf("invalid socket row or row limit")
	}
	slot := strings.TrimSuffix(fields[0], ":")
	if len(slot) > 10 {
		return socketRow{}, fmt.Errorf("socket slot too long")
	}
	if _, err := strconv.ParseUint(slot, 10, 32); err != nil {
		return socketRow{}, fmt.Errorf("socket slot: %w", err)
	}
	local, err := parseSocketAddress(fields[1], ipv6, order)
	if err != nil {
		return socketRow{}, err
	}
	remote, err := parseSocketAddress(fields[2], ipv6, order)
	if err != nil {
		return socketRow{}, err
	}
	if len(fields[3]) != 2 {
		return socketRow{}, fmt.Errorf("socket state requires two hex digits")
	}
	state, err := strconv.ParseUint(fields[3], 16, 8)
	if err != nil {
		return socketRow{}, fmt.Errorf("socket state: %w", err)
	}
	var inode uint64
	if len(fields) >= 10 {
		if len(fields[9]) > 20 {
			return socketRow{}, fmt.Errorf("socket inode too long")
		}
		inode, err = strconv.ParseUint(fields[9], 10, 64)
		if err != nil {
			return socketRow{}, fmt.Errorf("socket inode: %w", err)
		}
	}
	return socketRow{
		Local: local, Remote: remote, Protocol: protocol,
		StateCode: uint8(state), Inode: inode,
	}, nil
}

func parseSocketTable(
	data []byte,
	protocol uint8,
	ipv6 bool,
	order binary.ByteOrder,
	maxRows int,
) ([]socketRow, error) {
	if len(data) > maxSocketTableBytes || maxRows < 1 || maxRows > 262144 || order == nil ||
		(protocol != 6 && protocol != 17) {
		return nil, fmt.Errorf("invalid socket table bounds or protocol")
	}
	lines := strings.Split(string(data), "\n")
	header := strings.Fields(lines[0])
	if len(header) < 4 || header[0] != "sl" || header[1] != "local_address" ||
		(header[2] != "rem_address" && header[2] != "remote_address") ||
		header[3] != "st" {
		return nil, fmt.Errorf("missing socket table header")
	}
	var rows []socketRow
	for i, line := range lines {
		if len(line) > 4096 {
			return nil, fmt.Errorf("socket table line exceeds4096 bytes")
		}
		if i == 0 || i == len(lines)-1 && line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(rows) >= maxRows {
			return nil, errSocketRowLimit
		}
		row, err := parseSocketRow(fields, protocol, ipv6, order)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func tcpStateName(code uint8) string {
	names := [...]string{
		"",
		"ESTABLISHED",
		"SYN_SENT",
		"SYN_RECV",
		"FIN_WAIT1",
		"FIN_WAIT2",
		"TIME_WAIT",
		"CLOSE",
		"CLOSE_WAIT",
		"LAST_ACK",
		"LISTEN",
		"CLOSING",
		"NEW_SYN_RECV",
		"BOUND_INACTIVE",
	}
	if int(code) < len(names) && code != 0 {
		return names[code]
	}
	return fmt.Sprintf("UNKNOWN_%02X", code)
}

func parseSocketInode(target string) (uint64, bool, error) {
	if !strings.HasPrefix(target, "socket:") {
		return 0, false, nil
	}
	if !strings.HasPrefix(target, "socket:[") || !strings.HasSuffix(target, "]") ||
		len(target) > 29 {
		return 0, false, fmt.Errorf("malformed socket link")
	}
	inode, err := strconv.ParseUint(target[8:len(target)-1], 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("socket link inode: %w", err)
	}
	if inode == 0 {
		return 0, false, fmt.Errorf("socket inode must be positive")
	}
	return inode, true, nil
}

func parseProcessComm(data []byte) (string, error) {
	if len(data) > 256 || len(data) == 0 || !utf8.Valid(data) {
		return "", fmt.Errorf("invalid process command")
	}
	return strings.TrimSuffix(string(data), "\n"), nil
}

func parseEffectiveUID(data []byte) (string, error) {
	if len(data) > 65536 {
		return "", fmt.Errorf("process status too large")
	}
	found := false
	result := ""
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "Uid:") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "Uid:"))
		if found || len(fields) != 4 {
			return "", fmt.Errorf("invalid process Uid fields")
		}
		found = true
		for i, field := range fields {
			if len(field) > 10 {
				return "", fmt.Errorf("process UID too long")
			}
			uid, err := strconv.ParseUint(field, 10, 32)
			if err != nil {
				return "", fmt.Errorf("process UID: %w", err)
			}
			if i == 1 {
				result = strconv.FormatUint(uid, 10)
			}
		}
	}
	if !found {
		return "", fmt.Errorf("missing process Uid")
	}
	return result, nil
}
