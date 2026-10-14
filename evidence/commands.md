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
