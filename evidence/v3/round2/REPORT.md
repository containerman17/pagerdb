# Reuse commit buffers

2026-10-15 JST. Follow-up to the c8id snapshot allocation profile at slop 637aa79.
No new cloud run. macOS timing is diagnostic only and uses the shared benchmark
lock. Production throughput remains for the next cloud measurement.

The snapshot pool is replaced by three strongly retained ring slots. Each slot
keeps its dirty IDs and bytes until the disk consumer finishes. Storage grows
geometrically only when required and is never cleared on reuse. The existing
parallel copy remains. The ring bounds snapshots in flight; Commit waits for the
oldest consumer if all three are busy. This explicit backpressure replaces the
initial unbounded disk queue. A checkpoint can also hold a slot until its sync
finishes. There is still no per-block journal fsync.

Commit metadata and completion objects now use a fixed ring sized for retention
plus the in-flight work. Reusable WaitGroups replace allocated completion
channels. Undo and dirty buffers are exchanged, history keeps its capacity,
and workers reuse hash scratch space and callbacks. The disk queue and journal
assembly buffer are reused too. No disk format or public signature changes.

After warmup, BenchmarkCommitReusable reports **0 B/op, 0 allocs/op** for both
8 and 512 dirty leaves, 1000 blocks per run, three runs each. This measures the
whole write/Commit/Flush cycle, including hashing and journal append, so work is
not hidden after Commit returns. No checkpoint or epoch rotation occurs inside
this benchmark. Raw timings in benchmem.txt are diagnostics, not a throughput
claim or a substitute for the coordinator's cloud run.

```
python3 ~/.cache/cx/pdbv3-with-lock.py go test -run '^$' -bench '^BenchmarkCommitReusable$' -benchmem -benchtime=1000x -count=3
```

TestCommitAllocations requires zero allocations for 32 and 256 blocks at fixed
workload sizes, with GC between runs, serial/parallel workers and multiple RAM
retention settings. It also checks that snapshot addresses do not change.
TestSnapshotHighWater checks growth for larger blocks and retained capacity and
untouched spare bytes for smaller blocks. TestSnapshotRingBackpressure blocks the
disk, verifies three distinct immutable images, and checks that a fourth Commit
waits until reuse is safe. Rewind is tested after repeated metadata-ring wraps.

All prior tests remain. Local macOS arm64 and Linux arm64 vet, unit and race
checks pass. Growth, checkpoints and file rotation may still allocate; the zero
allocation result is for a warmed workload without those events. Real disk-full
behavior, physical power cuts and cloud throughput remain unverified here.
