# Epoch implementation evidence

Commits: bdd006f (streaming, compression, cumulative disk indexes and tests), 42dff34 (bounded page cache and newest matching root).

## Item 3

`epoch.go`, `epoch_sort.go`: emit fixed 40-byte references into scratch files inside the DB directory, sort 4096 records at a time, then merge sorted runs with reused buffers. Stream leaf frames to a temporary file and pass that file to EpochStore.Put. Roots and the cumulative index also stay on disk. Extra live memory is independent of epoch size. Scratch disk space scales with epoch size and the cumulative index; sealing does not promise bounded scratch disk.

`go test -run '^TestSealMemoryMeasure$' -v`: 1,048,576 bytes of log old/new payload measured 10,110,936 bytes peak extra heap; 8,388,608 bytes measured 14,182,392 bytes. Heap samples occur each millisecond; they measure runtime heap, including garbage awaiting collection, and exclude mmap-resident log pages and OS caches. Elapsed 20.400ms and 52.339ms. This is a sampling result, not a formal peak bound. The algorithm has fixed live buffers, one frame and one zstd encoder.

`TestDiskSort` verifies sorted output across multiple merge passes. `TestEpochCumulativeLookup` verifies views with unchanged leaves across six sealed epochs.

## Items 4 and 5

`epoch.go`: indexes remain in ReaderAt storage. The process-wide cache stores at most 64 index pages of 4096 bytes (256KiB). A second process-wide cache stores 64 reconstructed leaves (64KiB). Epoch pointer identity separates rewound histories. Epoch descriptors remain proportional to the number of epochs, but complete indexes never enter RAM.

The cumulative index stores one 24-byte row per leaf changed at or before this epoch. Rows contain leaf u32, this epoch's frame count u32, frame offset u64, latest changed epoch u64. Inherited rows have zero count and offset. Search epochs by their end version, then binary-search cumulative last-change epochs for the first epoch that changed the target leaf after the requested version. Each probe binary-searches that epoch's leaf index. Lookup is O(log epochs * log indexed leaves), instead of scanning later epochs. Frame selection within a leaf still scans frame headers. Sparse cumulative rows are duplicated across epochs, so index disk usage can grow as O(epochs * ever-changed leaves). This is a deliberate space tradeoff for bounded RAM and direct implementation.

`TestEpochIndexPageBoundary` checks records that cross a 4096-byte cache boundary and missing leaves. `TestEpochIndexValidation` rejects malformed index fields. `TestSealedRootReturnsNewest` verifies repeated equal roots resolve to the latest version, both directly in an epoch and through ViewRoot.

`go test -run '^$' -bench '^BenchmarkViewReadSealed$' -benchtime=200ms -count=3`: final 20.373-49.067us/read, 20,381-49,086 reads/s. Original 0776965 with the same command: 5.895-7.362us/read. A quiet original single run was 2.435us/read. The three-run measurements overlapped other benchmark activity. Disk index lookups and decompression still regress latency; no claim of parity is made. Baseline source was produced with `git archive 0776965` into a temporary subdirectory, then removed.

## Item 7

`epoch.go`: zstd SpeedFastest per frame with encoder concurrency one. Compression is used only when the stored payload is at most 90% of raw bytes. Frame flag 0 means raw, flag 1 means zstd. A process-wide decoder bounds retained decoder memory. Compressed input and decoded output have explicit size limits.

`go test -run '^$' -bench '^BenchmarkEpochCompression$' -benchtime=200ms -count=3`: EVM-shaped 64-byte writes with leading-zero 32-byte values store ratio 0.2363 and full 1000-entry frame decode 41.620-48.814us (1434-1682MB/s decoded). Random-byte writes store raw ratio 1.000 and decode 4.235-4.472us (15,657-16,535MB/s). This measures complete frame parsing/replay, not one point read. Full outputs are in epoch-compression.txt.

## Format and API changes

EpochStore gains durable Delete(epoch), implemented by LocalStore with directory sync and latest-epoch recalculation. Open must return an immutable snapshot until the reader closes. Removing an epoch suffix also removes its cumulative indexes; no sidecar rebuild is needed.

PGEPOCH2 is incompatible with PGEPOCH1. Header: 8-byte magic plus seven little-endian u64 fields: epoch, start version, end version, cumulative leaf count, index offset, root count, roots offset. Header size is 64 bytes. Frame header: flags u8, 3 zero bytes, stored payload bytes u32, first version u64. Raw or decompressed payload: 1024-byte base leaf, then entries of version delta uvarint, offset u16, length u16 and replacement bytes. Decoded frame limit is 263168 bytes. Sorted index rows are 24 bytes as described above. Root rows are hash[32] plus version u64, sorted by root with equal roots in chronological order. There is no leaf-hash section. Root count must equal end-start+1.

## Fuzz and race evidence

`GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzEpochFrame$' -fuzztime=2m`: PASS 121.400s, 4,980,172 executions, total corpus 19.

`GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzEpochIndex$' -fuzztime=2m`: PASS 120.440s, 4,577,236 executions, total corpus 11. The final page cache code was included in this run.

Each fuzzer has committed valid and truncated seed files under testdata/fuzz. Additional in-code seeds exercise parser entry points. Corpus counts include the native fuzz cache discovered during this session.

`go test -race -run 'Test(DiskSort|Epoch)|TestViews' -count=1`: PASS 27.439s. After the newest-root and cache-boundary tests, `go test -race -run 'Test(Epoch|SealedRootReturnsNewest)' -count=1`: PASS 1.936s.

Linux validation is owned by the coordinator. No Linux claim is made here.

## Seal base optimization

Sealing now uses each existing cumulative row's last-change epoch to read the checkpoint base directly from its immutable frame. It no longer walks queued later writes for every previously sealed leaf. Never-sealed leaves and genesis metadata still use the live-log reconstruction. The previous epoch's end must match the next checkpoint; missing epochs or indexed leaves return errors.

`TestSealBaseUsesPreviousChangeAndGenesis` verifies a leaf unchanged across three epochs, later head writes, first allocation of a new leaf, genesis metadata, and inconsistent base references. Focused race suite `go test -race -run 'Test(SealBase|EpochCumulative|Views|SealedRoot)' -count=1`: PASS12.673s.

`go test -run '^$' -bench '^BenchmarkSealBaseBacklog$' -benchtime=200ms -count=3`, with 10,000 pending writes after the checkpoint: former window reconstruction 160.720-203.250us/base; immutable sealed reconstruction 6.236-8.986us/base. This isolates base reconstruction and does not claim a sustained throughput result.
