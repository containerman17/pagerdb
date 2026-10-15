# Ordered dirty bitmap and EVM tail diagnosis

2026-10-15 JST. macOS arm64, Apple M3, Go 1.26.0. All benchmark commands
held ~/.cache/pagerdb-bench.lock. These are local diagnostics, not cloud throughput
claims. Baseline is core 589861c; cloud context is slop e4e6cc3.

## Change

Write/Touch now only set bitmap bits and extend the first/last dirty-word bounds.
Commit scans that range of uint64 words in order, extracts set bits with
TrailingZeros64, clears each word, and fills the snapshot slot's reusable ID
buffer. It no longer appends IDs on Write or sorts them on Commit. Snapshot copy
stays parallel; the three-slot ring, persistence behavior, public API, disk
format and roots are unchanged. The bitmap change is 21 added / 12 removed lines in pagerdb.go.
The undo follow-up adds 12 lines there. There is no production diagnostic hook or clock call.

The existing warmed allocation benchmark still reports **0 B/op, 0 allocs/op**
in all six 1000-block runs (8 and 512 leaves, three repetitions each). This
includes Write, Commit, hashing and journal drain. Growth, checkpoints and
rotation are outside that zero-allocation claim.

## Tail diagnosis

The diagnostic copy runs the existing BenchmarkEVM/hot_sigma_2000 workload:
2M slots, 50k writes/block, 700 blocks, retain 32, two runs per revision.
Each raw row records write and Commit time, undo-growth calls/time, observed GC
cycles and process-wide GC-assist CPU time, ring pending observations, and
calls/time for every explicit Commit wait: prior hash, ring, metadata hash/disk,
disk error mutex, parallel copy, and job submission. Growth timing covers the
Touch that exceeds the undo capacity, including its append and dirty marking.
The initial one-block Go calibration is excluded from metrics.json.

The write phase itself has no lock, channel or WaitGroup wait. All 1,224 undo
capacity growth events in each run occur in blocks 0 through 35: 34 per block
while the 36 metadata/undo buffers first fill. No undo growth occurs later.
Baseline write p99 is 2.210 / 1.896 ms; after those first 36 blocks it is
1.260 / 1.167 ms. All seven slowest write blocks in both baseline runs grow undo
buffers, and none finds a pending ring slot at Commit. Growth accounts for
roughly 0.87 to 1.26 ms of their 1.90 to 2.39 ms write phases.

With the bitmap, write p99 is 2.148 / 1.793 ms and warm p99 is 1.651 / 1.165 ms.
Ten of the fourteen slowest write blocks grow undo buffers. Four later
outliers in the first run (blocks 48, 51, 59 and 63) have no undo growth or ring
wait. Their cause is unresolved.
These Mac observations identify a local cold-buffer cost, but do not prove the
cause of the cloud's 7.5 ms write p99. The same script can test that on cloud.

Ring pending observations are 67 / 1 in the baseline and 8 / 1 with the bitmap.
Ring wait time averages 0.112319 / 0.000162 ms per block in the baseline and
0.007855 / 0.000343 ms with the bitmap. This variability is not a bitmap speedup
claim. The first baseline run briefly waits for disk consumption, but that cost
is in Commit, not its write tail. Other synchronization waits average less than
0.0012 ms/block individually; parallel copy averages 0.025 to 0.028 ms/block.
The raw rows and metrics.json retain counts and durations for every block.

No larger ring or journal pipeline was added: the evidence does not attribute
the write tail to ring/disk waits. Changing capacity would also increase retained
memory and could move a disk bottleneck without fixing it. The existing ring backpressure test remains in place. The coordinator then
requested the undo-growth fix below.

Instrumentation is confined to a disposable source copy. A slot's atomic busy
flag is sampled just before Wait; it can finish between that observation and
Wait, so pending observations are not exact scheduler-sleep counts. Wait durations
include call/clock overhead; slow-call counts mean at least 1 microsecond, not a
proof that the goroutine slept. GC-assist counters cover the process. Row
formatting and runtime metric reads occur after the measured write/Commit
phases, but can change pacing. They are included in benchmark wall time. No
instrumented throughput value should be used as a performance comparison.

## Undo follow-up

Undo storage now grows explicitly to max(required bytes, twice its capacity,
size hint), with one allocation/copy per growth. Commit records the smaller of
the last two committed undo lengths as the hint. A fresh buffer thus learns the
steady block size without propagating a single unusually large setup block.
Only the first write that needs capacity allocates; Commit just updates two
integers. Existing buffer swapping retains capacity across blocks. No option,
preallocated workload-sized ring, or new asynchronous worker was added.

The final diagnostic pair is in undo.txt. Growth drops from 1,224 to **52 events**
per 700-block run: 17 in the first unknown-sized block, one in each of blocks
1 through 35, then zero. Total measured growth time is 1.128 / 1.035 ms, versus
22.955 / 19.650 ms in the baseline. Write p99 is 1.220 / 1.678 ms, versus baseline
2.210 / 1.896 ms. Warm p99 is 1.216 / 1.683 ms. The second final run has later
write outliers without growth or ring waits; their cause remains unresolved.
These are diagnostics on the Mac, not a cloud performance guarantee.

Final ring pending observations are 22 / 0; ring wait averages 0.078170 /
0.000015 ms per block. Ring waits still belong to Commit. They do not explain
the remaining write outliers. The full per-block wait counters remain in the raw
output and metrics.json. BenchmarkCommitReusable still reports zero bytes and
zero allocations in all six warmed repeat runs after this change. The first
final-code invocation had one 512-leaf sample at 5 B/op with a rounded 0
allocs/op; it is retained in benchmem-first.txt. The other five samples were zero.
The repeat in benchmem.txt is zero in all six rows. Exact AllocsPerRun regression
checks also pass. The isolated nonzero byte sample is not attributed to a source;
these results support no recurring allocation per warmed block, not a claim
that every process-wide benchmark sample must always be exactly zero.

## Verification

Core vet, unit tests, race tests and no-cgo tests pass on macOS. The instrumented
copy also passes vet/race. TestDirtyBitmapOrder covers reverse and duplicate
marks, cross-word ranges, leaf zero, the final leaf, sparse ranges, empty commits,
word clearing and bound reset. Existing deterministic roots across worker counts,
Undo, Rewind, views, proofs, overlap, subprocess crash recovery and allocation
regressions pass. TestUndoGrowth checks doubling, at most one growth per
known-size cold block, no growth after ring reuse, and isolation from a large
setup block. Hosted Linux/macOS vet/race/no-cgo CI passes for the final production change:
https://github.com/containerman17/pagerdb/actions/runs/38720213504
Both full Wasm fixture pairs are checked after the pushed dependency is
available; their final results belong in the host's round3 report.

## Reproduction

Run each timing command under the shared benchmark lock on macOS. The local
helper uses fcntl.flock(LOCK_EX) on ~/.cache/pagerdb-bench.lock around one command.

```
python3 evidence/v3/round3/instrument.py . /tmp/pagerdb-baseline 589861c
python3 evidence/v3/round3/instrument.py . /tmp/pagerdb-bitmap 0e61c6d
python3 evidence/v3/round3/instrument.py . /tmp/pagerdb-undo
# Scratch source copies are not Git worktrees.
go test -C /tmp/pagerdb-baseline -run '^$' -bench '^BenchmarkEVM/hot_sigma_2000$' -benchtime=700x -count=2
go test -C /tmp/pagerdb-bitmap -run '^$' -bench '^BenchmarkEVM/hot_sigma_2000$' -benchtime=700x -count=2
go test -C /tmp/pagerdb-undo -run '^$' -bench '^BenchmarkEVM/hot_sigma_2000$' -benchtime=700x -count=2
python3 evidence/v3/round3/analyze.py evidence/v3/round3/baseline.txt evidence/v3/round3/bitmap.txt evidence/v3/round3/undo.txt
go test -run '^$' -bench '^BenchmarkCommitReusable$' -benchmem -benchtime=1000x -count=3
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
CGO_ENABLED=0 go test ./...
```

Cloud throughput and cloud tail attribution remain unverified. No cloud machine
was started. The earlier phase-2 scope stays deferred.
