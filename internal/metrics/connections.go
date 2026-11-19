package metrics

// ProcessOwner contains only known process metadata. User is a numeric effective
// UID; absent fields differ from empty names or invented PID/user zero values.
type ProcessOwner struct {
	Command *string
	PID     *uint32
	User    *string
}

// ListeningSocket is one TCP listener or locally bound UDP socket. Distinct
// sockets may have identical endpoints; shared sockets can have multiple owners.
type ListeningSocket struct {
	Local    Endpoint
	Protocol uint8
	Owners   []ProcessOwner
}

// TCPStateCount preserves canonical and future unknown TCP state names.
type TCPStateCount struct {
	State string
	Count uint64
}

// Connections is an instantaneous state, not a window average.
// Values and referenced slices/pointers are immutable after transfer.
type Connections struct {
	ListeningSockets []ListeningSocket
	TCPStates        []TCPStateCount
}

// ConnectionsObservation retains usable data when some tables/owners are missing.
type ConnectionsObservation struct {
	Data    Connections
	Partial bool
	Detail  string
}
