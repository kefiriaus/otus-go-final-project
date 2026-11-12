# System stats daemon

Финальный проект OTUS: сбор системной статистики и передача клиентам по gRPC.

The daemon runs the Linux collectors and serves the Stats API over gRPC.
The CLI client remains a scaffold until task 14.

## Структура

- `cmd/stats-daemon` — точка входа демона.
- `cmd/stats-client` — точка входа клиента.
- `internal/collector` — получение исходных измерений.
- `internal/aggregate` — история в памяти и расчёты за окно.
- `internal/config` — загрузка и проверка конфигурации.
- `internal/transport/grpc` — потоковая передача статистики.

## Проверка

Используется Go 1.27.3 (актуальный стабильный релиз на 2026-11-05).
Из корня репозитория:

```sh
go list ./...
go build ./...
go vet ./...
go test ./...
```

Контрактные тесты проверяют сериализацию, сохранение отсутствующих значений
и измеренных нулей. Streaming behavior is covered by transport tests.
Основная платформа — Linux; поддержка заполненности файловых систем
на macOS запланирована отдельно.

Сторонние библиотеки допускаются только для gRPC/Protobuf и их транзитивных
зависимостей. Сбор и агрегация используют стандартную библиотеку Go.

## API

[Protobuf-контракт, семантика полей и генерация](../api/stats/v1/README.md).
Исходник: `api/stats/v1/stats.proto`, Go-пакет: `pkg/api/stats/v1`.

## Конфигурация демона

Из корня репозитория:

```sh
go run ./cmd/stats-daemon -h
go run ./cmd/stats-daemon -port 50051 -config configs/stats-daemon.json
```

`-port` задаёт целый порт 1..65535, по умолчанию 50051. `-config` — путь
к JSON, относительно текущего рабочего каталога. Без флага файл не читается,
используются defaults. Явный пустой путь — ошибка. Повторный CLI-флаг
заменяет предыдущее значение. Позиционные аргументы не принимаются.

[Полный пример](../configs/stats-daemon.json) включает все шесть подсистем.
Частичная конфигурация для отключения CPU:

```json
{"subsystems":{"cpu":false},"max_window_seconds":60}
```

Все подсистемы по умолчанию включены: `load_average`, `cpu`, `disk_io`,
`filesystems`, `traffic`, `connections`. Каждая отключается независимо;
выключить все допустимо. Пропущенные поля сохраняют defaults, `{}` допустим.
`max_window_seconds` — целое 1..3600, по умолчанию 3600: предел клиентского M
и будущего хранения истории. Порт в JSON не задаётся.

Неизвестные поля, неверные типы, null вместо объекта/значений, пустой файл,
несколько JSON-документов и завершающий мусор отклоняются. Повторный ключ
заменяется последним значением (правило encoding/json). Ошибка файла
не подменяется defaults. Изменения применяются после перезапуска;
hot reload, переменные окружения и автоматический поиск файла не поддерживаются.

Exit codes:

- 0: help or normal SIGINT/SIGTERM cancellation.
- 2: CLI/configuration errors, reported on stderr.
- 1: listener, server, or terminal runtime failure, reported on stderr.

The configured port is bound on all available TCP interfaces. Subsystem switches
control which sources are constructed and started. Configuration applies at startup.

## Механизм сбора

`internal/collector` содержит независимый от Protobuf механизм сбора:

- `RunPeriodic[T]` получает типизированный `Source[T]`, выполняет первый замер
  сразу и следующие по фиксированной сетке. Defaults: период 1 секунда,
  таймаут замера 900 мс. Занятые такты пропускаются, вызовы не перекрываются.
- `Sample[T]` содержит UTC-время начала/окончания, значение и ошибку.
  При ошибке значение обнуляется; следующий замер продолжает работу.
- Вызывающий код владеет каналом результатов и закрывает его после остановки
  отправителей. Медленное чтение задерживает только соответствующий runner;
  отмена context прерывает ожидание отправки. Источник не изменяет возвращённые
  данные и обязан соблюдать context: произвольный Go-вызов принудительно
  остановить нельзя.
- `RunGroup` конкурентно запускает рабочие функции, при отмене ждёт их cleanup.
  Ошибки измерений передаются в Sample. Терминальный выход рабочей функции
  отменяет группу; неожиданный успешный выход тоже считается ошибкой.
  Пустая группа ждёт отмены. Непрерывный захват пакетов сможет использовать
  этот lifecycle отдельно от периодического сбора.
- `CommandRunner` запускает короткие команды напрямую без shell. Defaults:
  900 мс и по 1 MiB для stdout/stderr. При переполнении вывод обрезается,
  возвращается `ErrOutputLimit`; причина ошибки процесса сохраняется.
  Отмена завершает непосредственный процесс, а не дерево потомков.
  `WaitDelay` 100 мс ограничивает ожидание унаследованных потоков вывода.
  Непрерывный tcpdump требует отдельного streaming-механизма.

The daemon runtime connects these workers to bounded histories and the gRPC service.

## History and aggregation

`internal/aggregate` provides reusable core algorithms without Protobuf or gRPC:

- `Window` uses UTC half-open ranges `[start,end)`. `Schedule` anchors the first
  response to request acceptance plus M seconds and later deadlines to N seconds.
  `NextAfter` skips missed deadlines; it does not run timers or send responses.
- Each `History[T]` uses a fixed-capacity ring, retaining intervals intersecting
  a retention range ending at the latest accepted interval end and lasting
  `maxWindowSeconds + 2` seconds.
  The default series capacity is `maxWindowSeconds + 2`; dense input may evict
  entries earlier. Reads do not consume history and are safe alongside writes.
- `GaugeSeries[T]` holds one pending value and its bounded history. A value is
  usable until the next attempt, for at most one second. An error ends it early.
- `CounterSeries` holds one baseline and bounded rate history. Rates require
  adjacent successful points with unchanged generation/vector length, increasing
  counters, and at most 1.1 seconds between points. Errors or invalid continuity
  break the interval; a successful point establishes a fresh baseline. Subtraction
  precedes float conversion. Generation must identify resets that values alone
  cannot reveal. A closing counter observation may be exactly at window end.
- Adapters use measurement completion time (`collector.Sample.FinishedAt`) when
  application wiring converts measurements to events. Aggregate does not import
  collector. Event timestamps must increase strictly within each series.
- `WeightedMean` and `MeanRates` weight values by usable intersection duration;
  gaps are excluded from the denominator. Coverage divides usable duration by
  total window duration. `Latest` returns the last eligible state observed before
  window end, with coverage describing collection availability.
- `Summary.Data == nil` represents unavailable data; measured zero has present
  data. Partial inputs or incomplete time coverage produce `Partial=true`.
  The transport maps present results to OK/PARTIAL and empty results to
  UNAVAILABLE; DISABLED comes from configuration outside these algorithms.
- Callers must keep stored values and referenced slices/maps immutable, including
  values returned by reads. History copies interval records, not arbitrary nested
  storage. Counter baselines copy incoming counters, and rate reducers return a
  fresh vector. Entry counts are bounded; collectors must also bound the size of
  individual values and the number of device/flow series they create.

Metric-specific aggregation and daemon wiring are implemented below. Client output remains task 14.

## Linux load average

`collector.NewLoadAverageSource()` reads `/proc/loadavg` directly on Linux.
It requires mounted procfs and read access to that file, with no external
utilities or additional privileges. The source and its constructor are available
only on Linux; the domain type, parser, and aggregation code are portable.

The first three fields are the system's 1/5/15-minute load indicators. They are
not divided by CPU count or restricted to CPU percentages. File reads retain at
most 4097 bytes and reject input exceeding 4096 bytes. Missing files, read/close
errors, malformed values, nonfinite or negative numbers, and cancellation return
errors rather than successful zero measurements.

`metrics.LoadAverage` is independent of transport types. `aggregate.MeanLoadAverage`
computes each indicator's time-weighted mean using the same intervals and the
existing `WeightedMean` reducer. All three indicators share coverage, partial
status, and last-sample time. Valid zeros remain present measurements. A failed
attempt interrupts `GaugeSeries` continuity; a subsequent success restores it.

Collection checks cancellation before opening and after reading. A synchronous
procfs read cannot be forcibly interrupted; no background goroutine is abandoned
to simulate cancellation. `RunPeriodic` also checks its measurement deadline.

Fixture tests cover parsing, failures, recovery, and independent window reads.
`TestLoadAverageRealLinux` checks the actual kernel source without assuming fixed
load values; it skips only when the procfs load-average file is absent.
Application startup, configuration switches, server delivery, and client output
are wired through the daemon runtime; the client remains task 14.

## Linux CPU usage

`collector.NewCPUSource()` reads the first aggregate `cpu` line from `/proc/stat`.
It does not sum per-CPU lines or require external tools or extra privileges.
The source is Linux-only; the metric types, parser, and reducer are portable.
It accepts up to 4096 bytes in the first line, including its newline, and bounds
prefetch to 4097 bytes. Open/read/close failures, malformed counters, oversized
lines, and cancellation return errors instead of successful zero counters.
Synchronous procfs reads use the same cancellation limitations as load average.

`metrics.CPUCounters` retains eight raw uint64 categories until after subtraction:
user, nice, system, idle, iowait, irq, softirq, and steal. Missing trailing historical
categories default to zero; supplied trailing guest/future fields must be numeric.
Guest time is already included in user/nice and is not added a second time.
`aggregate.CPUCounterPoint` feeds these categories to the bounded `CounterSeries`.
Any raw decrease, including iowait decreasing, invalidates the interval. Errors
and long gaps break continuity; later successful points establish a new baseline.
Generation is process-local, and daemon restart starts a fresh baseline.

`aggregate.MeanCPUUsage` computes percentages from time-weighted category rates,
which is equivalent to the ratio of accumulated deltas over usable window
intersections. It does not average interval percentages or divide by CPU count.
A common scale keeps finite rate arithmetic bounded without changing the ratio.

- User includes nice.
- System includes irq and softirq.
- Idle includes only idle; iowait and steal have separate percentages.
- The denominator includes the eight categories exactly once. USER_HZ cancels.
- All-zero-total intervals have no usable distribution and are excluded from
  coverage. They do not become idle=100% or a present all-zero CPU distribution.
  With no usable intervals, data and last-sample time are absent. A measured zero
  user percentage with other active categories remains a valid value.

All five percentages share coverage, partial status, and last-sample time.
Fixture tests cover unequal-total weighting, clipped windows, guest columns,
resets, failures, recovery, and independent client reads. `TestCPURealLinux`
verifies the actual `/proc/stat` source; it skips only for absent procfs.
Application startup and gRPC delivery are provided by the daemon runtime.

## Linux disk I/O

`collector.NewDiskIOSource()` enumerates `/sys/block` and reads each whole device's
`dev` and `stat` files. Sysfs must be mounted and readable; no external tool or
additional privilege is required. Directory entries may be symlinks. Nested
partitions are not enumerated, and naming heuristics are not used. Whole virtual
devices such as loop, RAID, and device-mapper devices are included, with no
automatic sum across devices or storage layers.

Collection allows at most 128 devices, 4096 stat bytes, and 128 identity bytes.
Directory enumeration and file reads are bounded, including one extra item/byte
to detect overflow. Capacity or enumeration failures return an overall error and
no observation. Successful enumeration can return an empty list. A failure for
one device returns an explicit `DiskIOReading.Err` with zero counters while
other devices continue. Consumers must inspect these errors even when Collect
returns nil. Cancellation discards the observation; synchronous sysfs reads have
the same cancellation limitations as the procfs sources.

`DiskIOCounterPoint` preserves four raw components until after subtraction:
completed reads, completed writes, sectors read, and sectors written. It uses
canonical major:minor identity as the counter generation. Decreasing counters,
identity changes, errors, and long gaps invalidate the affected interval.
`MeanDiskIO` reports one device's time-weighted completed read+write operations/s
and KiB/s. Kernel sector counters always use 512-byte units, regardless of hardware
block size: one sector equals 0.5 KiB. Merges, discard/flush operations, timings,
and in-flight counts are not included in the reported rates. Unchanged counters
are valid zero activity with present data, unlike unavailable measurements.

The daemon maintains separate bounded device histories. Missing devices and
per-reading errors break only the affected baseline; global errors break all
baselines. The registry retains removed devices for relevant windows and expires
old entries. Empty enumeration, partial data, and unavailable rates remain distinct.

Identity is checked before and after the stat read; a changed identity rejects
that device reading. Major:minor identity and observed disappearance detect many
replacements, but a
same-name/same-identity replacement with higher counters between samples cannot
be reliably detected. Complete hotplug detection is not claimed.

Fixtures cover partitions, virtual devices, per-device failures, disappearance,
identity changes, resets, independent client windows, limits, and measured zero.
`TestDiskIORealLinux` checks actual sysfs enumeration and reports device failures;
it skips only when block enumeration is absent. Daemon and gRPC wiring are implemented in task 13.

## Linux filesystem usage

`collector.NewFilesystemsSource()` reads this process's `/proc/self/mountinfo` and
uses Linux `statfs` for each eligible visible mount. Mount points identify results;
bind mounts at different paths remain separate. Mountinfo escapes are decoded,
stacked mounts are resolved through parent relationships, and descendants hidden
by an overmount are excluded before filesystem-type filtering. Namespace mount
roots such as `net:[…]` are opaque metadata, not assumed to be absolute paths.

Service and virtual filesystems are excluded: proc, sysfs, tmpfs, devtmpfs, devpts,
cgroup/cgroup2, securityfs, pstore, debugfs, tracefs, configfs, fusectl, hugetlbfs,
mqueue, binfmt_misc, rpc_pipefs, nsfs, autofs, bpf, selinuxfs, efivarfs, ramfs,
rootfs, overlay, and aufs. Other types, including block-backed and network mounts,
remain eligible. An overlay-only container may therefore have no eligible mounts.

Mountinfo is limited to 1 MiB, individual lines to 64 KiB, records to 1024, and
eligible mounts to 128. Malformed, ambiguous, cyclic, or oversized mount tables
fail enumeration without partial results. Mount-specific statfs failures remain
explicit reading errors while other readings survive. A successfully enumerated
empty selection is a successful empty list. Procfs and statfs require read/access
permissions but no external utilities or additional privileges.

Used space is `(Blocks - Bfree) * accountingBlockSize`, with Frsize when positive
and Bsize otherwise. Bavail is deliberately ignored: reserved blocks must not
change the definition of used space. Used MiB divides bytes by 1024². Percentages
use total capacity, and are absent when total capacity is zero. Inode usage uses
`Files - Ffree`; zero or all-ones unavailable inode accounting produces absent
inode fields. Inconsistent total/free counts and nonpositive accounting block
sizes are errors; valid zero usage remains present data.

`aggregate.MeanFilesystemUsage` independently time-weights used MiB, used inode
counts, and each percentage. It does not calculate percentages from averaged
capacities. Optional fields use intervals where they are available; incomplete
optional coverage marks a result partial, while an unsupported field absent
throughout remains absent. Inode means may be fractional. Per-mount base coverage
and last-sample time follow the existing gauge rules.

Mount ID, major:minor, and mount root form a best-effort identity. A window uses
only its latest contributing identity, excluding previous backing mounts and
reflecting that time loss in coverage. Metadata must be consistent within that
identity. Mountinfo and statfs are not an atomic snapshot: remounts during a
collection, including same-identity replacements, cannot be fully detected.

The daemon bounds and expires mount histories and interrupts missing/failed mounts.
Overall collection failure interrupts every mount series. Schema and configuration
remain unchanged.

Cancellation is checked before enumeration, between mounts, and after statfs.
A synchronous filesystem operation can block, particularly on remote filesystems;
context cancellation cannot force it to return. No goroutines are abandoned to
simulate a hard timeout. macOS implementation and real second-OS execution remain
separate work; current source availability and real verification cover Linux.

## Linux network top talkers

`collector.NewTrafficCapture` creates a continuous worker using one tcpdump process
per selected interface. It does not use the periodic source or short-command
runner. Selection is fixed at construction: by default interfaces must be up and
not loopback. Explicit loopback selection requires `IncludeLoopback=true`.
At most eight interfaces are allowed; a larger default selection returns an
error, so callers must choose a smaller explicit set. Interface options are
currently programmatic; CLI/JSON and application wiring remain pending.

Capture runs directly without a shell, in nonpromiscuous mode, with both directions,
packet-buffered PCAP output and a 262144-byte snapshot limit. TCPdump must be installed
and have the host's packet-capture permissions (typically CAP_NET_RAW on Linux;
additional restrictions can depend on host policy). This code does not install
packages, grant capabilities, or elevate privileges. It retains only IP metadata,
not payloads or capture files. Multiple selected interfaces can observe the same
packet across different network layers; observations are not deduplicated. Select
an appropriate single interface when duplicate layer observations are unwanted.

Classic PCAP 2.4 in either byte order and microsecond/nanosecond precision is
supported. Ethernet/VLAN (up to four tags), raw IP, Linux cooked SLL/SLL2 and
explicit IPv4/IPv6 link types are decoded. IP byte counts include the IP header
and exclude link headers, VLAN tags, padding and declared FCS bytes. IPv6 extension
traversal is bounded at 16 headers. Protocol numbers remain open-ended. TCP/UDP
port 0 is present, while protocols without parsed ports use absent ports.

No fragment reassembly is attempted. Identifiable protocol bytes from fragments
are retained, with partial flow attribution when transport ports are unavailable.
Malformed frames, unsupported jumbo encodings, unsupported capture link formats,
and inconsistent lengths produce diagnostics/loss rather than invented values.
A truncated packet with sufficient headers may still supply its declared IP
length when consistent with the original wire length.

`aggregate.TrafficHistory` stores a fixed-capacity packet metadata ring with exact
capture timestamps and an explicit clock watermark. Defaults are 65536 packet
records and 4096 distinct tracked flows. Hard limits are 1048576 records and 65536
flows; retention is maximum M plus two seconds. Out-of-order timestamps are
accepted within retention; packet timestamps more than five seconds beyond the
watermark are rejected. Per-interface availability histories and coalesced loss
ranges are also bounded. Sustained high packet rates can exhaust packet retention
before M expires, making affected long-window results partial. Flow-cap rejection
preserves protocol bytes when packet capacity permits. Top-K truncation alone does
not mark data partial. Expired records release their flow references.

Queries use exact half-open windows without prorating packet-byte buckets.
Protocol totals and percentages use retained IP bytes. Flow rates divide retained
bytes by usable capture duration, rather than the packet span or full unavailable
window. Coverage is time with at least one selected interface active; missing
selected interfaces still make results partial, even when coverage is 1. Quiet
full capture yields present empty lists. No usable capture time yields absent data.
Last-sample time is the latest usable capture/checkpoint boundary, not the latest
packet. Sort order follows the API contract, including lexical canonical IP
strings and absent ports preceding present ones. Queries return independent slices. Open availability is clipped to the explicit
watermark; a query beyond the latest checkpoint is conservatively partial. The daemon serializes capture callbacks and fresh snapshot checkpoints, coalescing
stale heartbeat timestamps before calling the strict history API.

Each interface retries failures after one second by default (configurable from one
second through one minute). Failures remain internal to this worker so other metrics
can continue. A valid global PCAP header establishes readiness. Availability closes
at a detected failure/cancellation, before shutdown and pipe drain, and a late header
cannot reactivate a stopped session. Normal SIGINT shutdown drains final buffered
frames. After 500 ms the direct child is killed; forced shutdown or incomplete drains
mark results incomplete. All owned processes, pipes, timers and goroutines are joined.

SIGUSR1 requests kernel-drop statistics once per second. Increased drops use
conservative request-time boundaries rather than stderr receipt time. Missing or
malformed reports, rejected/delayed packets, decode errors, failed sessions, and
retention losses mark affected windows partial. Loss ranges can coalesce across
gaps and overstate incompleteness; they never promise full accounting after a
recorded loss. Sampling, packet arrival and loss reports are asynchronous, so later
reports may revise historical window completeness. No method can detect losses
that the OS does not report. LastError retains a bounded diagnostic and underlying
process errors. A broken sink is terminal; invalid/expired packet metadata is a
recoverable loss, distinguished by `metrics.ErrTrafficPacketRejected`.

Verification uses synthetic PCAP, helper processes, quiet capture, errors/retries,
stderr floods, clock boundaries, bounded churn and cancellation fixtures. Installed
tools here are tcpdump 4.99.6/libpcap 1.10.6; interface enumeration succeeded outside
the sandbox. The opt-in loopback test (`OTUS_TRAFFIC_SMOKE=1`) was attempted and
skipped because the current user lacks packet-capture permission. Actual live
capture is therefore not verified here. The selected flags and SIGUSR1 behavior
were checked against the upstream [tcpdump 4.9.2 manual](https://raw.githubusercontent.com/the-tcpdump-group/tcpdump/tcpdump-4.9.2/tcpdump.1.in)
and [source](https://raw.githubusercontent.com/the-tcpdump-group/tcpdump/tcpdump-4.9.2/tcpdump.c),
including initial header flushing with `-U`; this is not a real Ubuntu 18.04
execution result.

## Linux listening sockets and TCP states

`collector.NewConnectionsSource` reads the four `/proc/self/net/tcp`, `tcp6`, `udp`,
and `udp6` tables concurrently. Scope is the collector process's network namespace.
Procfs is used directly, without parsing ss/netstat output or requiring those tools.
IPv4 and IPv6 addresses use native byte order within 32-bit words; ports retain
their numeric values. TCP states are canonical strings, including LISTEN, and
unknown codes remain `UNKNOWN_XX`. Counts include every usable TCP row; UDP never
contributes TCP-state counts.

Output contains TCP LISTEN sockets and every UDP socket with a bound nonzero local
port, including connected UDP. Distinct socket inodes remain separate even when
they share an endpoint. Socket inode links under `/proc/<pid>/fd` identify owners;
repeated FDs from one PID are deduplicated, while shared sockets can have multiple
owners. FD directories stay open until their links are inspected, avoiding a
self-induced disappearance when scanning the collector's own directory FD.

Owner fields are optional. Command comes from comm (normally kernel-truncated to
15 bytes), with only its final newline removed. Invalid UTF-8 or unavailable
command/status files leave the corresponding field absent. User is the decimal
effective UID from the process's status, not the socket creator UID or a username.
No NSS/account-service lookup or privilege elevation occurs. Empty owner lists and
missing fields remain distinguishable from fabricated PID 0 or user 0 values.

Tables are limited to 4 MiB each and 4096-byte lines. Defaults bound 65536 combined
rows and 1024 output sockets; hard limits are 262144 and 4096. Listener output
truncation is partial but does not truncate TCP-state counts. A table read/parse
failure preserves usable sibling tables as partial data; all four unavailable is
an overall collection error. Valid empty tables are successful empty measurements.
Missing IPv6 files are diagnosed as unavailable, not assumed to mean zero sockets.

Owner lookup defaults: 4096 processes, 65536 total FD entries, 4096 FD entries per
process, 8 owners per socket, and 300 ms. Hard ceilings are 8192/131072/8192/32;
lookup time must be positive and at most one second. The process root also has a
non-PID entry budget, and all directory reads use bounded batches plus an overflow
detection entry. Comm/status/link limits are 256 bytes/64 KiB/4096 bytes. Diagnostic
text is bounded to 4096 bytes. No process cache or growing registry survives Collect.

Enrichment uses a separate deadline, reserving 50 ms before a parent deadline when
possible. Count/time limits, permissions, process/FD disappearance, malformed links
and unmatched listener inodes mark ownership partial while retaining known PIDs
and accessible fields. Parent cancellation aborts the whole observation and joins
the four table workers. Synchronous procfs operations are checked around calls;
context cancellation cannot force a blocked syscall to return.

`aggregate.LatestConnections` returns the last usable state before window end,
without averaging socket lists or state counts. Coverage measures periodic
availability, and source Partial flags propagate through Event.Partial. Returned
socket/state/owner slices and optional values are copied. Successful empty data
remain present; unavailable history remains absent. Application wiring must pass
observation.Data/Partial and collection errors into the corresponding gauge events.

Proc tables, FD links and process metadata are not an atomic snapshot. hidepid and
ptrace restrictions, short-lived sockets/processes, and PID/inode reuse limit
certainty; even one matched owner does not prove that no hidden owner exists.
The daemon enforces the connection history payload budget described below.
Collection and gRPC are wired; client output remains task 14.

Fixtures cover all table families, shared owners, duplicate endpoints, partial
metadata, budgets, cancellation, recovery and independent windows. The real Linux
read executed in the sandbox's namespace with empty socket tables. The opt-in
local-socket check (`OTUS_CONNECTIONS_SMOKE=1`) was run outside the sandbox after
local binds were denied inside it: four IPv4/IPv6 TCP/UDP sockets were verified and
all four matched this process's PID. Ownership remained partial because other
processes' FD directories were denied. This is not a claim of complete host-wide
ownership visibility or Ubuntu 18.04 runtime verification.


## Running the gRPC daemon

```sh
go build -o /tmp/stats-daemon ./cmd/stats-daemon
/tmp/stats-daemon -port 50051 -config configs/stats-daemon.json
```

Use an existing configuration file path, or omit `-config` for defaults. The daemon
binds `:PORT` without TLS or authentication; choose its network placement accordingly.
`stats.v1.StatsService.Subscribe` uses explicit `interval_seconds` (N) and
`window_seconds` (M). N is 1..3600; M is 1..`max_window_seconds`. Invalid requests
return InvalidArgument. N may exceed M. Every subscription waits its own M seconds
before the first response, regardless of existing history, then emits on an
acceptance-relative N-second schedule. Missed deadlines are skipped without bursts.
All six sections share the same UTC half-open window. Collection failures remain
section statuses, while invalid internal snapshots return Internal.

Collectors start immediately and run without clients. Ordinary sources sample
once per second with a 900 ms timeout; tcpdump capture runs continuously. Histories
are shared, and reading a snapshot never consumes another client's data. The store
serializes ingestion and snapshot construction; protobuf mapping and sending occur
outside its gate. Disabled sources are never constructed. Unsupported platform
sources are reported unavailable; real macOS filesystem support remains task 18.

Resource defaults are programmatic policies, not additional JSON keys:

- At most 32 active subscriptions across all connections; excess requests return
  ResourceExhausted. Each has one in-flight response and one replaceable pending
  response. A blocked Send times out after five seconds with DeadlineExceeded.
- Device and mount registries each retain at most 128 keys, including removed keys.
  Inactive keys expire after the configured maximum window plus two seconds.
  New keys at capacity are omitted with partial/unavailable reporting, not silently
  substituted for retained histories. Per-entity histories hold at most maximum
  window plus two entries; missing/error events break continuity.
- Connections retain at most 32 MiB of accounted payload and maximum window plus
  two observations. Accounting includes exact-length nested copies, fixed records,
  optional values, and strings, including the pending measurement. Allocator overhead,
  temporary snapshots, transport buffers, and other histories are separate: this is
  not an RSS limit. Oldest values are pruned on pressure, so long windows may have
  incomplete coverage. A single oversized observation is unavailable with a diagnostic.
- Traffic retains the existing 65536 packet records, 4096 flow keys, and at most
  eight interfaces; top-20 output truncation alone does not make data partial.
- Section diagnostics are limited to 512 bytes. Successful empty enumerations remain
  present data. Coverage excludes gaps and missing expected entities, and optional
  fields remain absent when unsupported.

SIGINT/SIGTERM cancels subscriptions and collectors. gRPC gets five seconds for
shutdown before forced Stop. Existing synchronous procfs/statfs calls still depend
on the operating system returning; context cancellation cannot interrupt a blocked
kernel syscall. Capture prerequisites and permission limitations remain as described
in the traffic section. No automatic privilege changes are made. The CLI client and
broader load/deployment work remain subsequent tasks.
