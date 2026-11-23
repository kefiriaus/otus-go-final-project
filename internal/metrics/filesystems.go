package metrics

// FilesystemUsage represents a mount's instantaneous usage or window means.
// Optional fields distinguish unsupported accounting and undefined percentages
// from measured zero. Values and referenced fields are immutable after transfer.
type FilesystemUsage struct {
	MountPoint        string
	Device            string
	FilesystemType    string
	Identity          string
	UsedMebibytes     float64
	UsedPercent       *float64
	UsedInodes        *float64
	UsedInodesPercent *float64
}

// FilesystemReading preserves one mount's failure independently of others.
type FilesystemReading struct {
	MountPoint string
	Usage      FilesystemUsage
	Err        error
}

// FilesystemObservation is a bounded selection of visible storage mounts.
type FilesystemObservation struct{ Readings []FilesystemReading }
