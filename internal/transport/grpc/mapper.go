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
	if math.IsNaN(s.CoverageRatio) || s.CoverageRatio < 0 || s.CoverageRatio > 1 || s.LastSampleAt == nil {
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
func mapSnapshot(v daemon.Snapshot) (*statsv1.Snapshot, error) {
	out := &statsv1.Snapshot{WindowStart: timestamppb.New(v.Window.Start), WindowEnd: timestamppb.New(v.Window.End), GeneratedAt: timestamppb.New(v.GeneratedAt)}
	for _, ts := range []*timestamppb.Timestamp{out.WindowStart, out.WindowEnd, out.GeneratedAt} {
		if err := ts.CheckValid(); err != nil {
			return nil, err
		}
	}
	if v.Window.Start.IsZero() || !v.Window.Start.Before(v.Window.End) || v.Window.End.Sub(v.Window.Start).Seconds() > 3600 || v.GeneratedAt.Before(v.Window.End) {
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
	fs, err := collectionStatus(v.Filesystems)
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
	for _, s := range []*statsv1.CollectionStatus{load, cpu, disk, fs, traffic, connections} {
		if s.LastSampleAt != nil && s.LastSampleAt.AsTime().After(v.Window.End) {
			return nil, fmt.Errorf("sample after window end")
		}
	}
	out.LoadAverage = &statsv1.LoadAverageSection{Status: load}
	out.Cpu = &statsv1.CpuSection{Status: cpu}
	out.DiskIo = &statsv1.DiskIoSection{Status: disk}
	out.Filesystems = &statsv1.FilesystemsSection{Status: fs}
	out.Traffic = &statsv1.TrafficSection{Status: traffic}
	out.Connections = &statsv1.ConnectionsSection{Status: connections}
	if d := v.LoadAverage.Summary.Data; d != nil {
		if !nonnegative(d.Load1, d.Load5, d.Load15) {
			return nil, fmt.Errorf("invalid load")
		}
		out.LoadAverage.Data = &statsv1.LoadAverage{Load1: d.Load1, Load5: d.Load5, Load15: d.Load15}
	}
	if d := v.CPU.Summary.Data; d != nil {
		if !nonnegative(d.UserPercent, d.SystemPercent, d.IdlePercent, d.IOWaitPercent, d.StealPercent) || d.UserPercent > 100 || d.SystemPercent > 100 || d.IdlePercent > 100 || d.IOWaitPercent > 100 || d.StealPercent > 100 {
			return nil, fmt.Errorf("invalid CPU percentage")
		}
		out.Cpu.Data = &statsv1.CpuUsage{UserPercent: d.UserPercent, SystemPercent: d.SystemPercent, IdlePercent: d.IdlePercent, IowaitPercent: d.IOWaitPercent, StealPercent: d.StealPercent}
	}
	if d := v.DiskIO.Summary.Data; d != nil {
		out.DiskIo.Data = &statsv1.DiskIoData{}
		for _, r := range *d {
			if r.Device == "" || !utf8.ValidString(r.Device) || !nonnegative(r.TransfersPerSecond, r.KibibytesPerSecond) {
				return nil, fmt.Errorf("invalid disk")
			}
			out.DiskIo.Data.Devices = append(out.DiskIo.Data.Devices, &statsv1.DiskIo{Device: r.Device, TransfersPerSecond: r.TransfersPerSecond, KibibytesPerSecond: r.KibibytesPerSecond})
		}
	}
	if d := v.Filesystems.Summary.Data; d != nil {
		out.Filesystems.Data = &statsv1.FilesystemsData{}
		for _, r := range *d {
			if !nonnegative(r.UsedMebibytes) || !percentage(r.UsedPercent) || !percentage(r.UsedInodesPercent) || r.UsedInodes != nil && !nonnegative(*r.UsedInodes) || r.UsedInodesPercent != nil && r.UsedInodes == nil || !utf8.ValidString(r.MountPoint+r.Device+r.FilesystemType) {
				return nil, fmt.Errorf("invalid filesystem")
			}
			out.Filesystems.Data.Filesystems = append(out.Filesystems.Data.Filesystems, &statsv1.FilesystemUsage{MountPoint: r.MountPoint, Device: r.Device, FilesystemType: r.FilesystemType, UsedMebibytes: r.UsedMebibytes, UsedPercent: clone(r.UsedPercent), UsedInodes: clone(r.UsedInodes), UsedInodesPercent: clone(r.UsedInodesPercent)})
		}
	}
	if d := v.Traffic.Summary.Data; d != nil {
		out.Traffic.Data = &statsv1.TrafficData{}
		for _, r := range d.Protocols {
			if !percentage(&r.Percent) {
				return nil, fmt.Errorf("invalid protocol percentage")
			}
			out.Traffic.Data.Protocols = append(out.Traffic.Data.Protocols, &statsv1.ProtocolTraffic{ProtocolNumber: uint32(r.Protocol), Bytes: r.Bytes, Percent: r.Percent})
		}
		for _, r := range d.Flows {
			if !nonnegative(r.BytesPerSecond) {
				return nil, fmt.Errorf("invalid flow rate")
			}
			a, err := endpoint(r.Flow.Source)
			if err != nil {
				return nil, err
			}
			b, err := endpoint(r.Flow.Destination)
			if err != nil {
				return nil, err
			}
			out.Traffic.Data.Flows = append(out.Traffic.Data.Flows, &statsv1.FlowTraffic{Source: a, Destination: b, ProtocolNumber: uint32(r.Flow.Protocol), BytesPerSecond: r.BytesPerSecond})
		}
	}
	if d := v.Connections.Summary.Data; d != nil {
		out.Connections.Data = &statsv1.ConnectionsData{}
		for _, r := range d.ListeningSockets {
			local, err := endpoint(r.Local)
			if err != nil {
				return nil, err
			}
			if !r.Local.HasPort || (r.Protocol != 6 && r.Protocol != 17) {
				return nil, fmt.Errorf("invalid socket")
			}
			sock := &statsv1.ListeningSocket{Local: local, ProtocolNumber: uint32(r.Protocol)}
			for _, owner := range r.Owners {
				if owner.PID != nil && *owner.PID == 0 || owner.Command != nil && !utf8.ValidString(*owner.Command) || owner.User != nil && !utf8.ValidString(*owner.User) {
					return nil, fmt.Errorf("invalid owner")
				}
				sock.Owners = append(sock.Owners, &statsv1.ProcessOwner{Command: clone(owner.Command), Pid: clone(owner.PID), User: clone(owner.User)})
			}
			out.Connections.Data.ListeningSockets = append(out.Connections.Data.ListeningSockets, sock)
		}
		for _, r := range d.TCPStates {
			if r.State == "" || !utf8.ValidString(r.State) {
				return nil, fmt.Errorf("invalid TCP state")
			}
			out.Connections.Data.TcpStates = append(out.Connections.Data.TcpStates, &statsv1.TcpStateCount{State: r.State, Count: r.Count})
		}
	}
	return out, nil
}
