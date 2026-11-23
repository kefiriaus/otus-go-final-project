package main

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
)

func formatEndpoint(endpoint *statsv1.Endpoint) (string, error) {
	if endpoint == nil {
		return "", fmt.Errorf("nil endpoint")
	}
	address, err := netip.ParseAddr(endpoint.Ip)
	if err != nil || address.Zone() != "" {
		return "", fmt.Errorf("invalid endpoint address %q", endpoint.Ip)
	}
	if endpoint.Port == nil {
		return address.String(), nil
	}
	if *endpoint.Port > 65535 {
		return "", fmt.Errorf("endpoint port %d exceeds 65535", *endpoint.Port)
	}
	if address.Is6() {
		return fmt.Sprintf("[%s]:%d", address.String(), *endpoint.Port), nil
	}
	return fmt.Sprintf("%s:%d", address.String(), *endpoint.Port), nil
}

func formatTraffic(section *statsv1.TrafficSection) (string, error) {
	if section == nil {
		return "", fmt.Errorf("nil traffic section")
	}
	status, err := formatStatus("traffic", section.Status)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	out.WriteString(status)
	if section.Data == nil {
		return out.String(), nil
	}
	if len(section.Data.Protocols) == 0 {
		out.WriteString("  protocols: empty\n")
	}
	for i, protocol := range section.Data.Protocols {
		if protocol == nil {
			return "", fmt.Errorf("nil traffic protocol at index %d", i)
		}
		fmt.Fprintf(&out, "  protocol=%d bytes=%d percent=%.2f%%\n",
			protocol.ProtocolNumber, protocol.Bytes, protocol.Percent)
	}
	if len(section.Data.Flows) == 0 {
		out.WriteString("  flows: empty\n")
	}
	for i, flow := range section.Data.Flows {
		if flow == nil {
			return "", fmt.Errorf("nil traffic flow at index %d", i)
		}
		source, err := formatEndpoint(flow.Source)
		if err != nil {
			return "", err
		}
		destination, err := formatEndpoint(flow.Destination)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&out, "  flow=%s -> %s protocol=%d bytes_s=%.2f\n",
			source, destination, flow.ProtocolNumber, flow.BytesPerSecond)
	}
	return out.String(), nil
}

func formatOwner(owner *statsv1.ProcessOwner) string {
	pid, command, user := notAvailable, notAvailable, notAvailable
	if owner.Pid != nil {
		pid = strconv.FormatUint(uint64(*owner.Pid), 10)
	}
	if owner.Command != nil {
		command = strconv.Quote(*owner.Command)
	}
	if owner.User != nil {
		user = strconv.Quote(*owner.User)
	}
	return fmt.Sprintf("pid=%s command=%s user=%s", pid, command, user)
}

func formatOwners(owners []*statsv1.ProcessOwner) string {
	formatted := make([]string, 0, len(owners))
	for _, owner := range owners {
		if owner == nil {
			formatted = append(formatted, "pid=n/a command=n/a user=n/a")
			continue
		}
		formatted = append(formatted, formatOwner(owner))
	}
	return "[" + strings.Join(formatted, "; ") + "]"
}

func formatConnections(section *statsv1.ConnectionsSection) (string, error) {
	if section == nil {
		return "", fmt.Errorf("nil connections section")
	}
	status, err := formatStatus("connections", section.Status)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	out.WriteString(status)
	if section.Data == nil {
		return out.String(), nil
	}
	if len(section.Data.ListeningSockets) == 0 {
		out.WriteString("  listening_sockets: empty\n")
	}
	for i, socket := range section.Data.ListeningSockets {
		if socket == nil {
			return "", fmt.Errorf("nil listening socket at index %d", i)
		}
		local, err := formatEndpoint(socket.Local)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&out, "  socket=%s protocol=%d owners=%s\n",
			local, socket.ProtocolNumber, formatOwners(socket.Owners))
	}
	if len(section.Data.TcpStates) == 0 {
		out.WriteString("  tcp_states: empty\n")
	}
	for i, state := range section.Data.TcpStates {
		if state == nil {
			return "", fmt.Errorf("nil TCP state at index %d", i)
		}
		fmt.Fprintf(&out, "  tcp_state=%s count=%d\n", state.State, state.Count)
	}
	return out.String(), nil
}
