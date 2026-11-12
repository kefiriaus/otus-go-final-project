package aggregate

import (
	"fmt"
	"slices"
	"unicode/utf8"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func cloneConnectionField[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// LatestConnections returns the last socket/state observation, never an average.
// Returned slices and optional owner fields do not alias retained history.
func LatestConnections(window Window, values []Interval[metrics.Connections]) (Summary[metrics.Connections], error) {
	for _, v := range values {
		states := make(map[string]bool, len(v.Value.TCPStates))
		for _, state := range v.Value.TCPStates {
			if state.State == "" || !utf8.ValidString(state.State) || states[state.State] {
				return Summary[metrics.Connections]{}, fmt.Errorf("invalid or duplicate TCP state")
			}
			states[state.State] = true
		}
		for _, socket := range v.Value.ListeningSockets {
			if !socket.Local.Address.IsValid() || socket.Local.Address.Zone() != "" || !socket.Local.HasPort || (socket.Protocol != 6 && socket.Protocol != 17) {
				return Summary[metrics.Connections]{}, fmt.Errorf("invalid listening socket endpoint or protocol")
			}
			for _, owner := range socket.Owners {
				if owner.PID != nil && *owner.PID == 0 || owner.Command != nil && !utf8.ValidString(*owner.Command) || owner.User != nil && !utf8.ValidString(*owner.User) {
					return Summary[metrics.Connections]{}, fmt.Errorf("invalid process owner metadata")
				}
			}
		}
	}
	result, err := Latest(window, values)
	if err != nil || result.Data == nil {
		return result, err
	}
	data := metrics.Connections{TCPStates: slices.Clone(result.Data.TCPStates), ListeningSockets: slices.Clone(result.Data.ListeningSockets)}
	for i := range data.ListeningSockets {
		data.ListeningSockets[i].Owners = slices.Clone(data.ListeningSockets[i].Owners)
		for j, owner := range data.ListeningSockets[i].Owners {
			data.ListeningSockets[i].Owners[j] = metrics.ProcessOwner{PID: cloneConnectionField(owner.PID), Command: cloneConnectionField(owner.Command), User: cloneConnectionField(owner.User)}
		}
	}
	result.Data = &data
	return result, nil
}
