# Verification commands

All commands ran from the repository with Go 1.26.0. Raw outputs in this directory
are the evidence; timings are wall-clock results on a shared Apple M3 host, not
isolated hardware comparisons. macOS arm64 uses 16384-byte OS pages. Linux arm64
and amd64 use 4096-byte OS pages (`getconf PAGESIZE`), Ubuntu 24.04, and kernel
7.0.14-orbstack-00380-ga7e0a2dc9535. Linux
amd64 runs through Rosetta, with `GODEBUG=asyncpreemptoff=1` because the compiler
crashed under translation with asynchronous preemption enabled. Native Intel
hardware is UNVERIFIED.

macOS commands use `TMPDIR=$PWD/.scratch/tmp`. Linux databases use the machine-local
`TMPDIR=/home/ilia/pagerdb/.scratch/tmp`. Linux source, Go SDK and build/module
caches were in the shared repository. The two temporary Linux machines are
`pagerdb-arm64` and `pagerdb-amd64`. Only these machines belong to this verification.

For Linux, replace the leading `go` in each command below with one of these
prefixes. Put `GOMAXPROCS` or `PAGERDB_*` assignments after the remote `env`:

```
orb -m pagerdb-arm64 env CGO_ENABLED=1 TMPDIR=/home/ilia/pagerdb/.scratch/tmp GOCACHE=/Users/ilia/pagerdb/.scratch/cache-arm64 GOMODCACHE=/Users/ilia/pagerdb/.scratch/mod-arm64 /Users/ilia/pagerdb/.scratch/linux-arm64/go/bin/go
orb -m pagerdb-amd64 env GODEBUG=asyncpreemptoff=1 CGO_ENABLED=1 TMPDIR=/home/ilia/pagerdb/.scratch/tmp GOCACHE=/Users/ilia/pagerdb/.scratch/cache-amd64 GOMODCACHE=/Users/ilia/pagerdb/.scratch/mod-amd64 /Users/ilia/pagerdb/.scratch/linux-amd64/go/bin/go
```

Forward-format evidence remains as a baseline. Final-format benchmark and full-suite
results use `xor` names. Native fault-handler tests use `native-uffd` names. Log,
block, rewind, and image parser fuzz results remain applicable because the XOR
change did not change those parsers. A `FAIL` or interrupted log is diagnostic
evidence, never a passing verification result.

The native UFFD tests require unrestricted userfaultfd. The temporary machines used:

```
orb -m pagerdb-arm64 sudo -n sysctl -w vm.unprivileged_userfaultfd=1
orb -m pagerdb-amd64 sudo -n sysctl -w vm.unprivileged_userfaultfd=1
```

## Checks

```
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
go test -run '^TestUserfaultfd$' -count=1 -v .
PAGERDB_RSS_TEST=1 go test -run '^TestViewMapSparseRSS$' -count=3 -v .
go test -run '^TestSealMemoryMeasure$' -count=3 -v .
```

The memory test reports sampled peak extra Go heap and total allocations, not OS
page cache or existing mmap/log indexes. Raw logs preserve each measured result.

## Fuzzing

```
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzLogRecords$' -fuzztime=2m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzBlockRecords$' -fuzztime=2m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzEpochFrame$' -fuzztime=2m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzEpochIndex$' -fuzztime=2m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzExportImage$' -fuzztime=2m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzRewindIntent$' -fuzztime=2m .
```

## Benchmarks

```
go test -run '^$' -bench '^(BenchmarkWrite64|BenchmarkCommit50k|BenchmarkViewReadWindow|BenchmarkViewReadSealed)$' -benchtime=2s -count=3 .
go test -run '^$' -bench '^(BenchmarkTreePath|BenchmarkTreeColdPath|BenchmarkEpochCompression|BenchmarkDurabilityLoop|BenchmarkExport|BenchmarkViewMapFirstTouch)$' -benchtime=100ms -count=3 .
go test -run '^$' -bench '^BenchmarkSustained$' -benchtime=10000x -count=3 -timeout=45m .
```

The sustained benchmark writes 64 bytes into 2400 leaves for each of 10000 blocks.
Background sealing runs every 100 versions. Throughput includes waiting for the
sealer to drain. Commit p50/p99 exclude the writes and final drain. Three runs are
independent databases. Advised-cold path benchmarks call MADV_DONTNEED outside the
timed interval; they do not evict the filesystem cache.

## Final backward-XOR measurements

```
go test -run '^$' -bench '^(BenchmarkWrite64|BenchmarkCommit50k|BenchmarkViewReadWindow|BenchmarkViewReadSealed|BenchmarkTreePath|BenchmarkDurabilityLoop|BenchmarkExport)$' -benchtime=1s -count=3 -timeout=30m .
go test -run '^$' -bench '^BenchmarkTreeColdPath$' -benchtime=100x -count=3 .
go test -run '^$' -bench '^(BenchmarkOpenVerified|BenchmarkViewMapFirstTouch)$' -benchtime=10x -count=3 .
PAGERDB_XOR_METRICS=1 go test -run '^TestXORWorkloadMetrics$' -count=3 -v -timeout=20m .
go test -run '^$' -bench '^BenchmarkXORSealedRead$' -benchtime=100x -count=3 -timeout=20m .
go test -run '^$' -bench '^BenchmarkSustained$' -benchtime=10000x -count=3 -timeout=45m .
```

XOR metrics use 160016 application writes across 16 leaves and 10001 versions.
Frame bytes per record include frame headers. The whole-file metric additionally
includes dense rows, frame directories, cumulative leaf indexes, roots, and
allocator metadata. The older versions are checked against captured expected bytes.
`BenchmarkXORSealedRead` calls the sealed reader directly so near-head reads do not
fall back to retained live logs. Public View.Read is independently checked.

The final memory campaign used three runs on every platform. The amd64 memory log
also includes sparse RSS tests; the dedicated `xor-linux-amd64-rss.txt` fresh-process
run supplies the reported RSS range. Final code verification is at commit 676c599.

## v2 final pass (2026-10-14 JST)

Code verified at commit 60d5083; later commits change only README and evidence.
Files prefixed `v2final-` are raw outputs from this pass. Every timed run waited
until no other `*.test` or dbreplay process used more than 20% CPU and held the
shared lock `~/.cache/pagerdb-bench.lock` while it ran. The host also ran two
unrelated long-lived node processes (about 1 core). macOS is the Apple M3 host;
Linux arm64 is the OrbStack machine `pagerdb-arm64` (Ubuntu 24.04, kernel
7.0.14-orbstack). Linux amd64 (Rosetta) ran correctness suites only.

Correctness (`v2final-{macos,linux-arm64,linux-amd64}-suite.txt`):

```
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
```

Linux command prefixes are in `linux-refresh-commands.md`; for Linux runs the
PAGERDB_* variables of a command go inside the machine's `env`, after the prefix's
`env`. Fuzzers
(`v2final-macos-fuzz.txt`), one minute each:

```
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzLogRecords$' -fuzztime=1m .
(same for FuzzBlockRecords, FuzzEpochFrame, FuzzEpochIndex, FuzzExportImage, FuzzRewindIntent)
```

Benchmarks, three runs each (`v2final-PLATFORM-GROUP.txt`, driven group by group):

```
bench-main:      go test -run '^$' -bench '^(BenchmarkWrite64|BenchmarkCommit50k|BenchmarkViewReadWindow|BenchmarkViewReadSealed|BenchmarkTreePath|BenchmarkDurabilityLoop|BenchmarkExport)$' -benchtime=1s -count=3 -timeout=30m .
bench-cold:      go test -run '^$' -bench '^BenchmarkTreeColdPath$' -benchtime=100x -count=3 .
bench-open:      go test -run '^$' -bench '^(BenchmarkOpenVerified|BenchmarkViewMapFirstTouch)$' -benchtime=10x -count=3 .
bench-xor:       go test -run '^$' -bench '^BenchmarkXORSealedRead$' -benchtime=100x -count=3 -timeout=30m .
bench-seal:      go test -run '^$' -bench '^BenchmarkSealEpoch$' -count=3 -timeout=30m .
bench-index:     go test -run '^$' -bench '^BenchmarkWindowIndexMemory$' -benchtime=1x -count=3 -timeout=30m .
bench-sustained: go test -run '^$' -bench '^(BenchmarkSustained|BenchmarkWriteCommitUntimedSeal|BenchmarkWriteCommitOnly)$' -benchtime=10000x -count=3 -timeout=60m .
xor-metrics:     PAGERDB_XOR_METRICS=1 go test -run '^TestXORWorkloadMetrics$' -count=3 -v -timeout=20m .
memory:          go test -run '^TestSealMemoryMeasure$' -count=3 -v -timeout=20m .
rss:             PAGERDB_RSS_TEST=1 go test -run '^TestViewMapSparseRSS$' -count=3 -v .
bench-write:     go test -run '^$' -bench '^BenchmarkWrite32Resident$' -benchtime=5000000x -count=3 -timeout=30m .
bench-evm:       PAGERDB_EVM_RETAIN=32 go test -run '^$' -bench '^BenchmarkEVM$' -benchtime=200x -count=3 -timeout=90m .
```

Write path before and after: `v2final-macos-write-before.txt` is bench-write at
0d4b417 with the 60d5083 benchmark file (which stops the timer before the deferred
Close; earlier bench-write numbers included Close and were too high), and
`v2final-macos-bench-evm-before.txt` is bench-evm at 0d4b417.
`v2final-macos-write-profile.txt` holds CPU profiles of the write loop at 0d4b417
and at 60d5083, with the commands.

`v2final-macos-sustained-interleaved.txt` alternates Sustained and
WriteCommitUntimedSeal (10000x, one run each, three pairs) at commit 7a733f0 and
logs any process above 20% CPU sampled every 2 s during each run.

`v2final-index-memory-{before,after}-macos.txt` and
`v2final-index-memory-rss-macos.txt` compare BenchmarkWindowIndexMemory at
c77f77c (before) and 7a733f0 (after the index shrink and version pruning); the
before run copied the benchmark file into a temporary worktree at c77f77c. The
RSS file reports `/usr/bin/time -l` maximum resident set size for the Retain32
case.

`v2final-macos-memory-plateau.txt` ran TestSealMemoryMeasure with a temporary
edit: writes `{8192, 32768, 65536, 131072, 262144}` and `MaxLeaves: 1 << 19`.
