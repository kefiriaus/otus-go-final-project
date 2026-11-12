package collector

import (
	"context"
	"fmt"
	"io/fs"
	"strconv"
	"time"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

func (s *ConnectionsSource) lookupOwners(parent context.Context, inodes []uint64) (map[uint64][]metrics.ProcessOwner, connectionIssues) {
	owners := make(map[uint64][]metrics.ProcessOwner, len(inodes))
	var issues connectionIssues
	if len(inodes) == 0 {
		return owners, issues
	}
	for _, inode := range inodes {
		if inode > 0 {
			owners[inode] = nil
		}
	}
	deadline := time.Now().Add(s.options.OwnerTimeout)
	if outer, ok := parent.Deadline(); ok && outer.Add(-50*time.Millisecond).Before(deadline) {
		deadline = outer.Add(-50 * time.Millisecond)
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	pids, _, err := s.numericDirectory(ctx, ".", s.options.MaxProcesses, 2*s.options.MaxProcesses+512, false)
	issues.add(err)
	totalFDs := 0
	for _, pid := range pids {
		if ctx.Err() != nil {
			issues.add(ctx.Err())
			break
		}
		if totalFDs >= s.options.MaxFDs {
			issues.add(fmt.Errorf("total process FD scan limit"))
			break
		}
		base := strconv.FormatUint(uint64(pid), 10)
		limit := min(s.options.MaxFDsPerProcess, s.options.MaxFDs-totalFDs)
		directory, err := s.procFS.Open(base + "/fd")
		if err != nil {
			issues.add(fmt.Errorf("open process FD directory %s: %w", base, err))
			continue
		}
		fds, scanned, err := s.numericEntries(ctx, directory, base+"/fd", limit, limit, true)
		totalFDs += scanned
		issues.add(err)
		matched := make(map[uint64]bool)
		for _, fd := range fds {
			if ctx.Err() != nil {
				issues.add(ctx.Err())
				break
			}
			name := base + "/fd/" + strconv.FormatUint(uint64(fd), 10)
			target, err := fs.ReadLink(s.procFS, name)
			if err != nil {
				issues.add(fmt.Errorf("process FD link %s: %w", name, err))
				continue
			}
			if len(target) > 4096 {
				issues.add(fmt.Errorf("process FD link exceeds byte limit"))
				continue
			}
			inode, socket, err := parseSocketInode(target)
			if err != nil {
				issues.add(err)
				continue
			}
			if !socket {
				continue
			}
			if _, wanted := owners[inode]; wanted {
				matched[inode] = true
			}
		}
		issues.add(directory.Close())
		if len(matched) == 0 {
			continue
		}
		knownPID := pid
		owner := metrics.ProcessOwner{PID: &knownPID}
		command, err := s.readFile(ctx, base+"/comm", 256)
		if err == nil {
			var value string
			value, err = parseProcessComm(command)
			if err == nil {
				owner.Command = &value
			}
		}
		issues.add(err)
		status, err := s.readFile(ctx, base+"/status", 65536)
		if err == nil {
			var value string
			value, err = parseEffectiveUID(status)
			if err == nil {
				owner.User = &value
			}
		}
		issues.add(err)
		for inode := range matched {
			if len(owners[inode]) >= s.options.MaxOwnersPerSocket {
				issues.add(fmt.Errorf("socket %d owner limit", inode))
				continue
			}
			owners[inode] = append(owners[inode], owner)
		}
	}
	if err := ctx.Err(); err != nil {
		issues.add(err)
	}
	for inode, list := range owners {
		if len(list) == 0 {
			issues.add(fmt.Errorf("socket %d has no accessible owner", inode))
		}
	}
	return owners, issues
}
