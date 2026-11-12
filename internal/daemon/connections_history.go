package daemon

import (
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/kefiriaus/otus-go-final-project/internal/aggregate"
	"github.com/kefiriaus/otus-go-final-project/internal/collector"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

type connectionCharge struct {
	at    time.Time
	bytes int
}
type connectionHistory struct {
	series                   *aggregate.GaugeSeries[metrics.Connections]
	charges                  []connectionCharge
	bytes, limit, maxEntries int
	retention                time.Duration
	diagnostic               string
}

// Clone exact-length storage so accounting cannot retain a larger backing slice.
func cloneConnections(v metrics.Connections) (metrics.Connections, int) {
	out := metrics.Connections{ListeningSockets: make([]metrics.ListeningSocket, len(v.ListeningSockets)), TCPStates: make([]metrics.TCPStateCount, len(v.TCPStates))}
	n := int(unsafe.Sizeof(out)) + int(unsafe.Sizeof(aggregate.Interval[metrics.Connections]{})) + int(unsafe.Sizeof(connectionCharge{}))
	n += len(out.ListeningSockets)*int(unsafe.Sizeof(metrics.ListeningSocket{})) + len(out.TCPStates)*int(unsafe.Sizeof(metrics.TCPStateCount{}))
	for i, state := range v.TCPStates {
		state.State = strings.Clone(state.State)
		out.TCPStates[i] = state
		n += len(state.State)
	}
	for i, socket := range v.ListeningSockets {
		socket.Owners = make([]metrics.ProcessOwner, len(v.ListeningSockets[i].Owners))
		n += len(socket.Owners) * int(unsafe.Sizeof(metrics.ProcessOwner{}))
		for j, owner := range v.ListeningSockets[i].Owners {
			if owner.PID != nil {
				p := *owner.PID
				socket.Owners[j].PID = &p
				n += 8
			}
			if owner.Command != nil {
				p := strings.Clone(*owner.Command)
				socket.Owners[j].Command = &p
				n += int(unsafe.Sizeof(p)) + len(p)
			}
			if owner.User != nil {
				p := strings.Clone(*owner.User)
				socket.Owners[j].User = &p
				n += int(unsafe.Sizeof(p)) + len(p)
			}
		}
		out.ListeningSockets[i] = socket
	}
	return out, n
}
func (h *connectionHistory) add(v collector.Sample[metrics.ConnectionsObservation]) error {
	h.diagnostic = detail(v.Err)
	retentionCutoff := v.FinishedAt.Add(-h.retention)
	cutoff := retentionCutoff
	value, charge := metrics.Connections{}, 0
	if v.Err == nil {
		value, charge = cloneConnections(v.Value.Data)
		if charge > h.limit {
			v.Err = fmt.Errorf("connection observation exceeds retention budget")
			h.diagnostic = detail(v.Err)
			value = metrics.Connections{}
			charge = 0
		}
	}
	for len(h.charges) > 0 && (!h.charges[0].at.After(retentionCutoff) || h.bytes+charge > h.limit || len(h.charges) >= h.maxEntries) {
		oldest := h.charges[0]
		h.bytes -= oldest.bytes
		copy(h.charges, h.charges[1:])
		h.charges[len(h.charges)-1] = connectionCharge{}
		h.charges = h.charges[:len(h.charges)-1]
		if end := oldest.at.Add(time.Second); end.After(cutoff) {
			cutoff = end
		}
	}
	if err := h.series.PruneBefore(cutoff); err != nil {
		return err
	}
	if err := h.series.Add(aggregate.Event[metrics.Connections]{At: v.FinishedAt, Value: value, Err: v.Err, Partial: v.Value.Partial}); err != nil {
		return err
	}
	if v.Err == nil {
		h.charges = append(h.charges, connectionCharge{v.FinishedAt, charge})
		h.bytes += charge
	}
	return nil
}
