package grpc

import (
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/kefiriaus/otus-go-final-project/internal/daemon"
	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func collectionStatus[T any](section daemon.Section[T]) (*statsv1.CollectionStatus, error) {
	s := section.Summary
	if !utf8.ValidString(section.Detail) || len(section.Detail) > 512 {
		return nil, fmt.Errorf("invalid collection detail")
	}
	out := &statsv1.CollectionStatus{Detail: section.Detail}
	if !section.Enabled || s.Data == nil {
		if s.Data != nil || s.CoverageRatio != 0 || s.LastSampleAt != nil || s.Partial {
			return nil, fmt.Errorf("unavailable section contains measurements")
		}
		out.State = statsv1.CollectionState_COLLECTION_STATE_UNAVAILABLE
		if !section.Enabled {
			out.State = statsv1.CollectionState_COLLECTION_STATE_DISABLED
		}
		return out, nil
	}
	if math.IsNaN(s.CoverageRatio) || s.CoverageRatio < 0 || s.CoverageRatio > 1 ||
		s.LastSampleAt == nil {
		return nil, fmt.Errorf("invalid collection coverage or sample time")
	}
	out.CoverageRatio = s.CoverageRatio
	out.LastSampleAt = timestamppb.New(*s.LastSampleAt)
	if err := out.LastSampleAt.CheckValid(); err != nil {
		return nil, err
	}
	out.State = statsv1.CollectionState_COLLECTION_STATE_OK
	if s.Partial || s.CoverageRatio < 1 {
		out.State = statsv1.CollectionState_COLLECTION_STATE_PARTIAL
	}
	return out, nil
}

func clone[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func nonnegative(values ...float64) bool {
	for _, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return false
		}
	}
	return true
}

func percentage(p *float64) bool { return p == nil || nonnegative(*p) && *p <= 100 }

func endpoint(e metrics.Endpoint) (*statsv1.Endpoint, error) {
	if !e.Address.IsValid() || e.Address.Zone() != "" {
		return nil, fmt.Errorf("invalid endpoint")
	}
	out := &statsv1.Endpoint{Ip: e.Address.String()}
	if e.HasPort {
		p := uint32(e.Port)
		out.Port = &p
	}
	return out, nil
}

func mapLoad(data *metrics.LoadAverage) (*statsv1.LoadAverage, error) {
	if data == nil {
		return nil, nil
	}
	if !nonnegative(data.Load1, data.Load5, data.Load15) {
		return nil, fmt.Errorf("invalid load")
	}
	return &statsv1.LoadAverage{Load1: data.Load1, Load5: data.Load5, Load15: data.Load15}, nil
}

func mapCPU(data *metrics.CPUUsage) (*statsv1.CpuUsage, error) {
	if data == nil {
		return nil, nil
	}
	values := []float64{
		data.UserPercent, data.SystemPercent, data.IdlePercent, data.IOWaitPercent, data.StealPercent,
	}
	if !nonnegative(values...) {
		return nil, fmt.Errorf("invalid CPU percentage")
	}
	for _, value := range values {
		if value > 100 {
			return nil, fmt.Errorf("invalid CPU percentage")
		}
	}
	return &statsv1.CpuUsage{
		UserPercent: data.UserPercent, SystemPercent: data.SystemPercent,
		IdlePercent: data.IdlePercent, IowaitPercent: data.IOWaitPercent,
		StealPercent: data.StealPercent,
	}, nil
}

func mapDisks(data *[]metrics.DiskIO) (*statsv1.DiskIoData, error) {
	if data == nil {
		return nil, nil
	}
	out := &statsv1.DiskIoData{}
	for _, r := range *data {
		if r.Device == "" || !utf8.ValidString(r.Device) ||
			!nonnegative(r.TransfersPerSecond, r.KibibytesPerSecond) {
			return nil, fmt.Errorf("invalid disk")
		}
		out.Devices = append(out.Devices, &statsv1.DiskIo{
			Device: r.Device, TransfersPerSecond: r.TransfersPerSecond,
			KibibytesPerSecond: r.KibibytesPerSecond,
		})
	}
	return out, nil
}

func mapFilesystems(data *[]metrics.FilesystemUsage) (*statsv1.FilesystemsData, error) {
	if data == nil {
		return nil, nil
	}
	out := &statsv1.FilesystemsData{}
	for _, r := range *data {
		if !nonnegative(r.UsedMebibytes) || !percentage(r.UsedPercent) ||
			!percentage(r.UsedInodesPercent) ||
			r.UsedInodes != nil && !nonnegative(*r.UsedInodes) ||
			r.UsedInodesPercent != nil && r.UsedInodes == nil ||
			!utf8.ValidString(r.MountPoint+r.Device+r.FilesystemType) {
			return nil, fmt.Errorf("invalid filesystem")
		}
		out.Filesystems = append(out.Filesystems, &statsv1.FilesystemUsage{
			MountPoint: r.MountPoint, Device: r.Device, FilesystemType: r.FilesystemType,
			UsedMebibytes: r.UsedMebibytes, UsedPercent: clone(r.UsedPercent),
			UsedInodes: clone(r.UsedInodes), UsedInodesPercent: clone(r.UsedInodesPercent),
		})
	}
	return out, nil
}

func mapTraffic(data *metrics.Traffic) (*statsv1.TrafficData, error) {
	if data == nil {
		return nil, nil
	}
	out := &statsv1.TrafficData{}
	for _, r := range data.Protocols {
		if !percentage(&r.Percent) {
			return nil, fmt.Errorf("invalid protocol percentage")
		}
		out.Protocols = append(out.Protocols, &statsv1.ProtocolTraffic{
			ProtocolNumber: uint32(r.Protocol), Bytes: r.Bytes, Percent: r.Percent,
		})
	}
	for _, r := range data.Flows {
		if !nonnegative(r.BytesPerSecond) {
			return nil, fmt.Errorf("invalid flow rate")
		}
		source, err := endpoint(r.Flow.Source)
		if err != nil {
			return nil, err
		}
		destination, err := endpoint(r.Flow.Destination)
		if err != nil {
			return nil, err
		}
		out.Flows = append(out.Flows, &statsv1.FlowTraffic{
			Source: source, Destination: destination, ProtocolNumber: uint32(r.Flow.Protocol),
			BytesPerSecond: r.BytesPerSecond,
		})
	}
	return out, nil
}

func mapConnections(data *metrics.Connections) (*statsv1.ConnectionsData, error) {
	if data == nil {
		return nil, nil
	}
	out := &statsv1.ConnectionsData{}
	for _, r := range data.ListeningSockets {
		local, err := endpoint(r.Local)
		if err != nil {
			return nil, err
		}
		if !r.Local.HasPort || (r.Protocol != 6 && r.Protocol != 17) {
			return nil, fmt.Errorf("invalid socket")
		}
		socket := &statsv1.ListeningSocket{Local: local, ProtocolNumber: uint32(r.Protocol)}
		for _, owner := range r.Owners {
			if owner.PID != nil && *owner.PID == 0 ||
				owner.Command != nil && !utf8.ValidString(*owner.Command) ||
				owner.User != nil && !utf8.ValidString(*owner.User) {
				return nil, fmt.Errorf("invalid owner")
			}
			socket.Owners = append(socket.Owners, &statsv1.ProcessOwner{
				Command: clone(owner.Command), Pid: clone(owner.PID), User: clone(owner.User),
			})
		}
		out.ListeningSockets = append(out.ListeningSockets, socket)
	}
	for _, r := range data.TCPStates {
		if r.State == "" || !utf8.ValidString(r.State) {
			return nil, fmt.Errorf("invalid TCP state")
		}
		out.TcpStates = append(out.TcpStates, &statsv1.TcpStateCount{State: r.State, Count: r.Count})
	}
	return out, nil
}

func sanitizeSnapshotReadings(v daemon.Snapshot) daemon.Snapshot {
	if v.DiskIO.Enabled && v.DiskIO.Summary.Data != nil {
		data := *v.DiskIO.Summary.Data
		filtered := make([]metrics.DiskIO, 0, len(data))
		for _, r := range data {
			if utf8.ValidString(r.Device) {
				filtered = append(filtered, r)
			}
		}
		if len(filtered) != len(data) {
			v.DiskIO.Summary.Data = &filtered
			v.DiskIO.Summary.Partial = true
		}
	}
	if v.Filesystems.Enabled && v.Filesystems.Summary.Data != nil {
		data := *v.Filesystems.Summary.Data
		filtered := make([]metrics.FilesystemUsage, 0, len(data))
		for _, r := range data {
			if utf8.ValidString(r.MountPoint) && utf8.ValidString(r.Device) &&
				utf8.ValidString(r.FilesystemType) {
				filtered = append(filtered, r)
			}
		}
		if len(filtered) != len(data) {
			v.Filesystems.Summary.Data = &filtered
			v.Filesystems.Summary.Partial = true
		}
	}
	return v
}

func mapSnapshot(v daemon.Snapshot) (*statsv1.Snapshot, error) {
	v = sanitizeSnapshotReadings(v)
	out := &statsv1.Snapshot{
		WindowStart: timestamppb.New(v.Window.Start), WindowEnd: timestamppb.New(v.Window.End),
		GeneratedAt: timestamppb.New(v.GeneratedAt),
	}
	for _, ts := range []*timestamppb.Timestamp{out.WindowStart, out.WindowEnd, out.GeneratedAt} {
		if err := ts.CheckValid(); err != nil {
			return nil, err
		}
	}
	if v.Window.Start.IsZero() || !v.Window.Start.Before(v.Window.End) ||
		v.Window.End.Sub(v.Window.Start).Seconds() > 3600 || v.GeneratedAt.Before(v.Window.End) {
		return nil, fmt.Errorf("invalid snapshot window")
	}
	load, err := collectionStatus(v.LoadAverage)
	if err != nil {
		return nil, err
	}
	cpu, err := collectionStatus(v.CPU)
	if err != nil {
		return nil, err
	}
	disk, err := collectionStatus(v.DiskIO)
	if err != nil {
		return nil, err
	}
	filesystems, err := collectionStatus(v.Filesystems)
	if err != nil {
		return nil, err
	}
	traffic, err := collectionStatus(v.Traffic)
	if err != nil {
		return nil, err
	}
	connections, err := collectionStatus(v.Connections)
	if err != nil {
		return nil, err
	}
	for _, s := range []*statsv1.CollectionStatus{load, cpu, disk, filesystems, traffic, connections} {
		if s.LastSampleAt != nil && s.LastSampleAt.AsTime().After(v.Window.End) {
			return nil, fmt.Errorf("sample after window end")
		}
	}
	out.LoadAverage = &statsv1.LoadAverageSection{Status: load}
	out.Cpu = &statsv1.CpuSection{Status: cpu}
	out.DiskIo = &statsv1.DiskIoSection{Status: disk}
	out.Filesystems = &statsv1.FilesystemsSection{Status: filesystems}
	out.Traffic = &statsv1.TrafficSection{Status: traffic}
	out.Connections = &statsv1.ConnectionsSection{Status: connections}
	if out.LoadAverage.Data, err = mapLoad(v.LoadAverage.Summary.Data); err != nil {
		return nil, err
	}
	if out.Cpu.Data, err = mapCPU(v.CPU.Summary.Data); err != nil {
		return nil, err
	}
	if out.DiskIo.Data, err = mapDisks(v.DiskIO.Summary.Data); err != nil {
		return nil, err
	}
	if out.Filesystems.Data, err = mapFilesystems(v.Filesystems.Summary.Data); err != nil {
		return nil, err
	}
	if out.Traffic.Data, err = mapTraffic(v.Traffic.Summary.Data); err != nil {
		return nil, err
	}
	if out.Connections.Data, err = mapConnections(v.Connections.Summary.Data); err != nil {
		return nil, err
	}
	return out, nil
}
