package collector

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/kefiriaus/otus-go-final-project/internal/metrics"
)

// ConnectionsOptions bounds one socket snapshot and its best-effort owner scan.
// OwnerTimeout must be positive and no greater than one second.
type ConnectionsOptions struct {
	MaxSocketRows       int
	MaxListeningSockets int
	MaxProcesses        int
	MaxFDs              int
	MaxFDsPerProcess    int
	MaxOwnersPerSocket  int
	OwnerTimeout        time.Duration
}

// DefaultConnectionsOptions returns conservative per-collection work bounds.
func DefaultConnectionsOptions() ConnectionsOptions {
	return ConnectionsOptions{65536, 1024, 4096, 65536, 4096, 8, 300 * time.Millisecond}
}

// ConnectionsSource reads this process's network namespace and accessible owners.
// Construct it with NewConnectionsSource. It has no persistent process cache.
type ConnectionsSource struct {
	procFS  fs.FS
	options ConnectionsOptions
}

// NewConnectionsSource validates work limits without opening sockets or files.
func NewConnectionsSource(o ConnectionsOptions) (*ConnectionsSource, error) {
	if o.MaxSocketRows < 1 || o.MaxSocketRows > 262144 || o.MaxListeningSockets < 1 ||
		o.MaxListeningSockets > 4096 ||
		o.MaxProcesses < 1 ||
		o.MaxProcesses > 8192 ||
		o.MaxFDs < 1 ||
		o.MaxFDs > 131072 ||
		o.MaxFDsPerProcess < 1 ||
		o.MaxFDsPerProcess > 8192 ||
		o.MaxOwnersPerSocket < 1 ||
		o.MaxOwnersPerSocket > 32 ||
		o.OwnerTimeout <= 0 ||
		o.OwnerTimeout > time.Second {
		return nil, fmt.Errorf("invalid connections work limits")
	}
	return &ConnectionsSource{procFS: os.DirFS("/proc"), options: o}, nil
}

type connectionIssues struct {
	partial bool
	detail  string
}

func (i *connectionIssues) add(err error) {
	if err == nil {
		return
	}
	i.partial = true
	if len(i.detail) >= 4096 {
		return
	}
	text := strings.ToValidUTF8(err.Error(), "�")
	if i.detail != "" {
		text = "; " + text
	}
	remaining := 4096 - len(i.detail)
	if len(text) > remaining {
		text = text[:remaining]
		for !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
	}
	i.detail += text
}

func (s *ConnectionsSource) readFile(ctx context.Context, name string, limit int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := s.procFS.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open proc %s: %w", name, err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, int64(limit+1)))
	if err := errors.Join(readErr, file.Close(), ctx.Err()); err != nil {
		return nil, fmt.Errorf("read proc %s: %w", name, err)
	}
	if len(data) > limit {
		return nil, fmt.Errorf("proc %s exceeds %d-byte limit", name, limit)
	}
	return data, nil
}

// numericDirectory reads bounded batches, retaining entries found before a limit
// or read error. One extra entry detects overflow; nonnumeric entries also count.
func (s *ConnectionsSource) numericDirectory(
	ctx context.Context,
	name string,
	maxNumbers, maxEntries int,
	allowZero bool,
) (numbers []uint32, scanned int, result error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	file, err := s.procFS.Open(name)
	if err != nil {
		return nil, 0, fmt.Errorf("open proc directory %s: %w", name, err)
	}
	defer func() { result = errors.Join(result, file.Close(), ctx.Err()) }()
	return s.numericEntries(ctx, file, name, maxNumbers, maxEntries, allowZero)
}

func numericProcEntry(value string, allowZero bool) (uint32, bool, error) {
	if value == "" {
		return 0, true, nil
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return 0, true, nil
		}
	}
	if len(value) > 10 {
		return 0, false, fmt.Errorf("oversized numeric proc entry")
	}
	number, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, false, fmt.Errorf("numeric proc entry: %w", err)
	}
	if number == 0 && !allowZero {
		return 0, true, nil
	}
	return uint32(number), false, nil
}

func (s *ConnectionsSource) numericEntries(
	ctx context.Context,
	file fs.File,
	name string,
	maxNumbers, maxEntries int,
	allowZero bool,
) (numbers []uint32, scanned int, result error) {
	defer func() { slices.Sort(numbers); numbers = slices.Compact(numbers) }()
	dir, ok := file.(fs.ReadDirFile)
	if !ok {
		return nil, 0, fmt.Errorf("proc %s is not an enumerable directory", name)
	}
	for {
		if err := ctx.Err(); err != nil {
			return numbers, scanned, err
		}
		batch, readErr := dir.ReadDir(min(128, maxEntries+1-scanned))
		for _, entry := range batch {
			scanned++
			if scanned > maxEntries {
				return numbers, scanned, fmt.Errorf("proc %s entry scan limit", name)
			}
			number, skip, err := numericProcEntry(entry.Name(), allowZero)
			if err != nil {
				return numbers, scanned, err
			}
			if skip {
				continue
			}
			if len(numbers) >= maxNumbers {
				return numbers, scanned, fmt.Errorf("proc %s numeric entry limit", name)
			}
			numbers = append(numbers, number)
		}
		if errors.Is(readErr, io.EOF) {
			return numbers, scanned, nil
		}
		if readErr != nil {
			return numbers, scanned, fmt.Errorf("enumerate proc %s: %w", name, readErr)
		}
		if len(batch) == 0 {
			return numbers, scanned, io.ErrNoProgress
		}
	}
}

var _ Source[metrics.ConnectionsObservation] = (*ConnectionsSource)(nil)

type socketTableResult struct {
	rows []socketRow
	err  error
}

func (s *ConnectionsSource) readSocketTables(ctx context.Context) [4]socketTableResult {
	tables := [4]struct {
		name     string
		protocol uint8
		ipv6     bool
	}{{"tcp", 6, false}, {"tcp6", 6, true}, {"udp", 17, false}, {"udp6", 17, true}}
	var results [4]socketTableResult
	var workers sync.WaitGroup
	for i, table := range tables {
		workers.Add(1)
		go func() {
			defer workers.Done()
			data, err := s.readFile(ctx, "self/net/"+table.name, maxSocketTableBytes)
			if err == nil {
				results[i].rows, err = parseSocketTable(
					data, table.protocol, table.ipv6, binary.NativeEndian, s.options.MaxSocketRows,
				)
			}
			if err != nil {
				results[i].err = fmt.Errorf("socket table %s: %w", table.name, err)
			}
		}()
	}
	workers.Wait()
	return results
}

func summarizeSocketRows(results [4]socketTableResult) (map[string]uint64, []socketRow, connectionIssues) {
	states := make(map[string]uint64)
	seen := make(map[uint64]bool)
	var candidates []socketRow
	var issues connectionIssues
	for _, result := range results {
		for _, row := range result.rows {
			if row.Inode > 0 {
				if seen[row.Inode] {
					issues.add(fmt.Errorf("duplicate socket inode %d", row.Inode))
					continue
				}
				seen[row.Inode] = true
			}
			if row.Protocol == 6 {
				states[tcpStateName(row.StateCode)]++
			}
			if row.Protocol == 6 && row.StateCode == 10 ||
				row.Protocol == 17 && row.Local.Port != 0 {
				candidates = append(candidates, row)
			}
		}
	}
	return states, candidates, issues
}

// Collect reads four tables concurrently and enriches retained socket owners.
// Ownership limits/failures preserve data as partial; parent cancellation does
// not. Procfs reads and process metadata are best-effort, not an atomic snapshot.
func (s *ConnectionsSource) Collect(ctx context.Context) (metrics.ConnectionsObservation, error) {
	if err := ctx.Err(); err != nil {
		return metrics.ConnectionsObservation{}, err
	}
	results := s.readSocketTables(ctx)
	if err := ctx.Err(); err != nil {
		return metrics.ConnectionsObservation{}, err
	}
	var issues connectionIssues
	valid, total := 0, 0
	var failures []error
	for _, result := range results {
		if result.err != nil {
			if errors.Is(result.err, errSocketRowLimit) {
				return metrics.ConnectionsObservation{}, result.err
			}
			issues.add(result.err)
			failures = append(failures, result.err)
		} else {
			valid++
			total += len(result.rows)
		}
	}
	if valid == 0 {
		return metrics.ConnectionsObservation{}, fmt.Errorf(
			"no usable socket tables: %w",
			errors.Join(failures...),
		)
	}
	if total > s.options.MaxSocketRows {
		return metrics.ConnectionsObservation{}, errSocketRowLimit
	}
	states, candidates, rowIssues := summarizeSocketRows(results)
	if rowIssues.partial {
		issues.add(errors.New(rowIssues.detail))
	}
	slices.SortFunc(candidates, func(a, b socketRow) int {
		if c := strings.Compare(a.Local.Address.String(), b.Local.Address.String()); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Local.Port, b.Local.Port); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Protocol, b.Protocol); c != 0 {
			return c
		}
		return cmp.Compare(a.Inode, b.Inode)
	})
	if len(candidates) > s.options.MaxListeningSockets {
		issues.add(fmt.Errorf("listening socket output limit"))
		candidates = candidates[:s.options.MaxListeningSockets]
	}
	var inodes []uint64
	targets := make(map[uint64]bool)
	for _, row := range candidates {
		if row.Inode == 0 {
			issues.add(fmt.Errorf("listener has unavailable socket inode"))
			continue
		}
		if !targets[row.Inode] {
			inodes = append(inodes, row.Inode)
			targets[row.Inode] = true
		}
	}
	owners, ownerIssues := s.lookupOwners(ctx, inodes)
	if ownerIssues.partial {
		issues.add(errors.New(ownerIssues.detail))
	}
	if err := ctx.Err(); err != nil {
		return metrics.ConnectionsObservation{}, err
	}
	data := metrics.Connections{
		ListeningSockets: make([]metrics.ListeningSocket, 0, len(candidates)),
		TCPStates:        make([]metrics.TCPStateCount, 0, len(states)),
	}
	for _, row := range candidates {
		data.ListeningSockets = append(
			data.ListeningSockets,
			metrics.ListeningSocket{
				Local:    row.Local,
				Protocol: row.Protocol,
				Owners:   slices.Clone(owners[row.Inode]),
			},
		)
	}
	for name, count := range states {
		data.TCPStates = append(data.TCPStates, metrics.TCPStateCount{State: name, Count: count})
	}
	slices.SortFunc(
		data.TCPStates,
		func(a, b metrics.TCPStateCount) int { return strings.Compare(a.State, b.State) },
	)
	return metrics.ConnectionsObservation{
		Data:    data,
		Partial: issues.partial,
		Detail:  issues.detail,
	}, nil
}
