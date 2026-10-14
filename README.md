# PagerDB

PagerDB stores regions of bytes, versions, and SHA-256 roots. It has no knowledge of
what the bytes mean. Go 1.26, Linux and macOS. Dependencies are the standard library,
`golang.org/x/sys/unix`, and `github.com/klauspost/compress/zstd`.

## Basic use

```go
db, err := pagerdb.Open(dir, pagerdb.Options{})
if err != nil { return err }
defer db.Close()
r, err := db.Alloc(1024)
if err != nil { return err }
db.Write(r, 0, []byte("hello"))
root, version, err := db.Commit()
if err != nil { return err }
view, err := db.View(version)
if err != nil { return err }
defer view.Close()
buf := make([]byte, 5)
if err = view.Read(r, 0, len(buf), buf); err != nil { return err }
proof, err := db.Proof(r, 0)
if err != nil { return err }
if !pagerdb.Verify(root, proof) { return errors.New("invalid proof") }
```

`go test -run '^Example$'` runs the complete example in `export_test.go`.

## Design

A **leaf** is 1024 bytes at a fixed offset in `state`. Its hash is SHA-256 of those
bytes. Parent hashes are SHA-256 of the concatenated child hashes. Empty subtree
hashes are precomputed. The root covers allocation metadata and data. Tree capacity
is a power of two, fixed at creation. The default is 2^24 leaves (16 GiB); supported
capacities are 2^13 through 2^32 leaves. The tree is sparse and uses 4 KiB tiles with
seven levels (127 hashes) per full tile. A path at the default capacity visits four
tiles. The hash calculation is identical to the binary heap tree.

A **region** is a stable uint32 handle. Sizes through 1 MiB use power-of-two classes
starting at 1 KiB. Larger regions use chains of 2 MiB segments. Each small segment
tracks its live slot count. Freeing its last slot removes its free-list entries and
returns the segment to the common pool. Region table segments are allocated on
demand, with 131072 entries per segment. Freeing and growing are logged operations.
`Map` provides contiguous shared bytes, including nonadjacent large-region segments.
Call `Touch` before every direct write to those bytes.

A **commit** completes pending Touch records, hashes dirty leaves and their paths,
and appends a version/root record with CRC-32C of the entries. Hashing is parallel
across dirty nodes. `Undo(Mark())` restores pending writes. `Rewind(v)` restores any
version within `RetainVersions` (default 4096), including versions in sealed epochs.
It deletes the replaced sealed suffix and keeps earlier history. A durable rewind
intent lets Open finish an interrupted rewind. Open views prevent a rewind that
would invalidate their versions. Whole log chunks stay until their versions leave
retention and no view needs them. Per-leaf undo references for retained log chunks
remain in RAM and scale with the retained writes.

**Sealing** runs in the background after `EpochVersions` (default 10000) or
`EpochBytes` (default 1 GiB). It writes compact log references to temporary files,
partitions contiguous leaf ranges across up to GOMAXPROCS workers, radix-sorts
fixed 32768-record runs, and performs two-way external merges. Each worker writes
its section through large buffers. Frames, indexes and roots stream through disk
staging into `EpochStore.Put`. Reused sorting and compression buffers bound memory
per worker independently of epoch size. Scratch disk
space grows with the epoch. Rotation waits when two epochs are active or queued,
so a fast writer cannot create an unbounded sealing backlog. This can delay rotation
commits when sealing or storage is slow. Recovery may load an older backlog; new
rotations wait for it to drain. The limit is two active and queued epochs combined. `Seal()` waits for the queue and returns storage errors.

Sealed history stores **backward XOR records**. The head is the base. Each write
becomes old XOR new, trimmed to its first and last nonzero byte. No-op writes have
no record. Per-leaf records run newest first, including reverse write order within
a version. A full run of 16 KiB ends with a leaf copy after undoing that run. Across
epochs, the sealer charges each changed epoch its final-run bytes plus 256 bytes
per epoch hop, plus 16 KiB if the epoch had several runs. When the charge since the
leaf's last base copy reaches 16 KiB, that epoch's final run also ends with a leaf
copy: the leaf state at the epoch start. A leaf changed once per epoch therefore
gets a base copy about every 60 changed epochs. Frames use fastest-level zstd only
when it saves at least 10%.

Each epoch has a sorted sparse cumulative leaf index. Its latest-change and
previous-change pointers lead directly to epochs where a leaf changed. Indexes,
frame directories, and dense version/range lists stay on disk. Epoch files from
`LocalStore` are mapped read-only, so the OS page cache keeps hot index pages and
raw frames are read in place; other stores go through a process-wide 2 MiB
index-page cache. Decoders and decode buffers are pooled, so sealed reads do not
allocate. A read first undoes live writes to the newest sealed
boundary, then follows base pointers to the oldest base copy at or after the
requested version, using two index lookups per base. From that copy it walks
changed epochs backward to the requested version. Each hop uses O(log indexed
leaves) lookup. Within an epoch, it binary-searches the dense change list, skips
payloads with no overlapping change, and uses a newer full-run copy when available.
The decode work after the copy is bounded by the 16 KiB charge plus at most two
frames in the target epoch, so deep reads cost the base hops plus one bounded walk.
Work grows with relevant history, not with untouched epochs.
Cumulative index rows trade extra disk space for direct lookup.

A contiguous prefix of old epoch files can be deleted while the database is closed.
Open accepts a missing prefix and rejects an interior gap. No rebasing is needed.
Versions older than the first remaining checkpoint remain available only while
retained live logs cover them. Rewind retention still depends on retained live logs; it does not automatically delete
sealed history.

A **view** reads the requested version using log undo bytes or sealed frames.
Window reads copy log-covered bytes directly, without first racing a head `Map`
write to those same bytes. `View.Map` is writable and private. On Linux with cgo it registers
an anonymous mapping with userfaultfd and supplies one OS page on each missing-page
fault. A native pthread reads from an immutable historical snapshot and calls
UFFDIO_COPY. It never calls Go. A Go handler is unsafe here: a fault can block a Go
thread inside an unpreemptible byte operation while garbage collection waits for
that thread, preventing the Go handler from running. Historical changed leaves are
therefore reconstructed into the snapshot file during Map setup, not on demand in
a Go fault handler. Only accessed pages enter the returned mapping. Without cgo,
or when unrestricted userfaultfd is unavailable, Linux uses the correct private-file
fallback. User-mode-only userfaultfd is insufficient because ordinary file I/O can
access a Go slice from the kernel. The native path needs permission for kernel-mode
fault handling, such as vm.unprivileged_userfaultfd=1 on the test machines.

macOS has no userfaultfd. It clones the state file with the native filesystem clone
operation, eagerly patches only leaves changed after the requested version in the
snapshot file, then privately maps the region. The independent clone is necessary: MAP_PRIVATE of the live
file alone does not isolate pages the view has not faulted yet. If filesystem cloning
is unavailable, a bounded-buffer sparse file copy provides the same semantics.
Unchanged mapping pages remain demand-paged. Snapshot files are unlinked after
setup. Linux native fault threads keep their snapshot descriptor until view close. Closing a view releases its mappings and history pin. Setup metadata scales with
changed leaves, region segments and the sealed epoch count; the page payload buffer is fixed.

**Export** streams the metadata and all nonzero state leaves at a version. It
recomputes and checks the exported root. **Import** verifies the image against the
caller's expected root, stages a new sparse state file, then opens a database at the
exported version with the same region IDs. It does not include earlier history.
Both use bounded buffers, independent of image size.

## Durability and ownership

`Sync: true` makes undo records durable before any corresponding shared state byte
can change, with one log barrier (msync and fsync) per block. Write and allocator
writes stage their new bytes in memory instead of the shared state mapping. Read,
Views, allocation and hashing see staged bytes through an overlay. Commit writes the
block record, makes the whole block durable with one barrier, then applies the
staged bytes to the mapping. Touch logs old bytes and makes the log durable (one
barrier) before returning, and applies staged writes first, because the caller then
writes the mapping directly. DB.Map applies staged writes the same way. A DB.Map
slice therefore shows Write bytes from the next Commit, Touch or Map call on; until
then use Read. Undo drops staged bytes without touching the mapping. Any state page
can reach disk at any time, but it never holds a byte whose undo record is not
durable. Log bytes that an undo clears are included in the next barrier, so undone
entries cannot reappear as a torn tail. Recovery replays complete commits and undoes
unfinished writes. State/tree checkpoints and directory
changes are synced before old recovery data can be discarded. Under the operating
system's filesystem sync guarantees, acknowledged commits survive power loss.
A committed record that reached storage immediately before a crash can also recover
even if the caller did not receive Commit's return value.

`Sync: false` skips per-write and per-commit durability barriers. A clean-shutdown
marker is removed and its removal synced before opening the database for use.
Close syncs the files before creating the marker. The marker records the closed
version and root with a checksum. A clean open checks the marker, the log head and
the stored tree root against each other and then trusts the stored tree. Any
mismatch, a damaged marker or a missing marker selects the unclean path, which
rebuilds the tree from state bytes and requires the recorded root. `DB.Verify`
rehashes the whole state on demand for operators. An unclean open either recovers a
verified version or returns an error naming the recovery problem. Lost commits are
possible in this mode. Lost log bytes with surviving state changes cannot silently
produce an accepted mismatched head. Detection does not repair missing undo data.

One goroutine owns mutations and the head Read/Root/Version API. Views can read from
other goroutines while the owner writes. Every direct head mapping write must have
an earlier Touch covering its entire range. Serialize View.Map creation and Close
with owner mutations. Do not access a mapping after its view closes. Close views
before closing the DB. Serialize access to an individual writable private mapping
as for any Go byte slice. The database directory must have only one open writer;
external file mutation is unsupported. Storage must honor sync and atomic rename.
The race detector runs on the real code, with no race suppression.

## API

```go
Open(dir string, options Options) (*DB, error)
Import(dir string, r io.Reader, expectedRoot [32]byte) (*DB, error)
(*DB) Close() error
(*DB) Alloc(size int) (Region, error)
(*DB) Grow(region Region, size int) error
(*DB) Free(region Region) error
(*DB) Map(region Region) ([]byte, error)
(*DB) Read(region Region, off, n int, buf []byte)
(*DB) Write(region Region, off int, data []byte)
(*DB) Touch(region Region, off, n int)
(*DB) Mark() Mark
(*DB) Undo(mark Mark)
(*DB) Commit() ([32]byte, uint64, error)
(*DB) Root() [32]byte
(*DB) Version() uint64
(*DB) Rewind(version uint64) error
(*DB) Seal() error
(*DB) Export(version uint64, w io.Writer) error
(*DB) View(version uint64) (*View, error)
(*DB) ViewRoot(root [32]byte) (*View, error)
(*DB) Proof(region Region, off int) (Proof, error)
(*DB) Verify() error
Verify(root [32]byte, proof Proof) bool
(*View) Read(region Region, off, n int, buf []byte) error
(*View) Copy(region Region) ([]byte, error)
(*View) Map(region Region) ([]byte, error)
(*View) Version() uint64
(*View) Close()
```

Options are `MaxLeaves`, `Sync`, `RetainVersions`, `EpochVersions`, `EpochBytes`, and
`Store`. Zero values select the defaults described above; Sync defaults to false.
Head range violations panic like slice bounds violations. Proof and Verify reject pending
uncommitted writes. DB.Map slices are invalidated by Free/Grow or allocation rollback.
View.Map snapshots remain valid until View.Close. Mutation methods without an error
return can panic on storage failures, including log growth and Touch barriers in Sync mode.
Undo can also panic if restoring durable state fails. Commit can advance the
version and then return a rotation or sealing error; inspect its returned version.
Import requires a destination that does not exist and is not created concurrently.
The expected root must identify a valid PagerDB state, not arbitrary attacker-created
allocator metadata.

Pluggable epoch storage implements:

```go
type EpochStore interface {
    Put(epoch uint64, r io.Reader) error
    Open(epoch uint64) (io.ReaderAt, int64, error)
    Latest() uint64
    Delete(epoch uint64) error
}
```

Put and Delete must be durable when they return. Epoch numbers form a consecutive
sequence starting at one. Rewind deletes a suffix; offline history pruning can
delete a contiguous prefix. Open returns an error matching os.ErrNotExist for a
missing epoch. Returned readers must remain
valid until closed. Readers implementing io.Closer are closed by the DB.
`NewLocalStore(dir)` supplies durable local files.

## Exact file formats

All integers are little endian unless stated otherwise. Padding bytes are zero.
OS page size is a host property, never a format constant. State version 2 and epoch version 3
use explicit magic bytes; version 1 state files are rejected.

### State

| Byte offset | Bytes | Field |
|---|---:|---|
| 0 | 8 | `PAGERDB2` |
| 8 | 8 | MaxLeaves |
| 16 | 8 | segment high-water count, including header |
| 24 | 8 | next never-used region ID |
| 32 | 8 | free region ID stack head |
| 40 | 8 | free segment chain head |
| 48 | 176 | 11 classes, each free slot head u64 then carve offset u64 |
| 224 | 800 | reserved zeros |
| 1024 | 8 * maxSegments | segment next pointer, or live count for a small-class segment |
| regTab | 8 * ceil(MaxLeaves / 131072) | region-table segment directory |

`maxSegments = MaxLeaves * 1024 / 2097152` and
`regTab = alignUp(1024 + 8*maxSegments, 1024)`. Header size rounds up to 2 MiB.
Directory entry `region / 131072` contains the segment number, or zero if absent.
Within that segment, entry `region % 131072` is exactly 16 bytes:
`kind u32 | segment count u32 | slot offset or first segment u64`.
Kind 0 is free, 1..11 are the small classes, and 12 is a segment chain.
Free entries store the next free ID in their final u64. Free small slots store their
next free-slot offset in the first eight bytes. Region ID zero is reserved.

### Tree

Each full 4096-byte tile contains 127 consecutive 32-byte hashes in local heap
order, followed by 32 zero bytes. The partial root tile contains
`depth % 7 + 1` levels; lower tiles each contain seven. Tile roots are in breadth-first
order. For global heap node `i`, let `d=floor(log2(i))`, `top=depth%7+1`.
If `d < top`, its byte offset is `(i-1)*32`. Otherwise:

```
k          = (d-top)/7
rootDepth  = top+7*k
localDepth = d-rootDepth
prefix     = i >> localDepth
before     = 1 + 2^top * (2^(7*k)-1)/127
tile       = before + prefix - 2^rootDepth
local      = 2^localDepth + (i & (2^localDepth-1))
offset     = tile*4096 + (local-1)*32
```

An all-zero hash slot denotes the precomputed empty subtree hash at its level.
An unclean open rebuilds the tree from state bytes, so the tree is a derived file.

### Log chunks

Names are zero-padded twelve-digit epoch numbers under `log/`. Records are packed,
with no alignment gaps. The tag is written after the other bytes.

```
checkpoint, 56 bytes:
  0 'C', 1..7 padding, 8 version u64, 16 root [32],
  48 CRC-32C of bytes 8..47 u32, 52..55 padding
entry, 32+2*n bytes:
  0 'E', 1..3 padding, 4 region u32, 8 n u32, 12 undo CRC-32C u32,
  16 version u64, 24 state offset u64, 32 old [n], 32+n new [n]
commit, 56 bytes:
  0 'B', 1..3 padding, 4 entry count u32, 8 version u64,
  16 root [32], 48 CRC-32C u32, 52..55 padding
```

CRC-32C uses the Castagnoli polynomial. The entry undo checksum covers bytes
4..11, 16..31, and the old bytes. The commit checksum covers all complete entry
bytes since the preceding checkpoint or commit, then commit bytes 4..47. A chunk starts with one checkpoint. Unused tail
bytes are zero. A tag alone is not a durability barrier; Sync performs explicit
log synchronization before staged bytes reach the shared state mapping.

`rewind` is a 32-byte durable intent: `PGREWND2 [8] | target version u64 |
target epoch u64 | target log end u64`. The log end is immediately after the target
commit/checkpoint. Recovery repeats restoration before removing the intent.
`clean` is 52 bytes: `PGCLEAN1 [8] | version u64 | root [32] | CRC-32C of bytes
0..47 u32`. A valid marker records a completed clean close at that head.

### Sealed epochs

The epoch magic is `PGEPOCH4`. The state format remains `PAGERDB2`. Header, 64 bytes:

```
0  "PGEPOCH4" [8]
8  epoch u64
16 checkpoint version u64
24 end version u64
32 cumulative indexed leaf count u64
40 index byte offset u64
48 root record count u64
56 root table byte offset u64
```

After the header, each worker section contains frames, frame directories, and
dense change lists for a contiguous leaf range. Sections appear in leaf order,
followed by the cumulative leaf index and roots. All stored offsets are absolute
file offsets. Each frame starts with this 32-byte header:

```
0 flags u8 (bit 0: zstd, bit 1: closing leaf copy), 1..3 zero padding
4 stored payload bytes u32
8 decoded payload bytes u32
12 record count u32
16 first record index within this leaf u64
24 oldest relative version u32
28 newest relative version u32
```

Decoded records are `relative version u32 | offset u16 | length u16 | XOR [length]`.
Versions are relative to the checkpoint, nonzero, and descending. Equal versions
retain reverse write order. Leading and trailing zero XOR bytes are removed; a
record's first and last XOR bytes must therefore be nonzero. Touch can finish after later
writes, so stored new bytes alone do not identify each intermediate state. Sealing
starts a fixed-size working leaf at the epoch end, then computes each record as
working XOR old and replaces working with old. This preserves the live log's exact
newest-first undo semantics, including overlapping deferred Touch operations.
The run contains at most 16384 record bytes. A full run appends a 1024-byte leaf
copy representing the state after undoing that run. Every nonfinal run has a copy;
the final run has one exactly when the index row names this epoch as the leaf's
base epoch. Copies occur at the end, never as frame bases.
The decoded payload is at most 17408 bytes. Compression uses zstd fastest only
when compressed bytes are at most 90% of raw bytes. No other flag bits are valid.

Each frame directory row is 24 bytes:
`frame offset u64 | first record index u64 | record count u32 | flags u32`.
Each dense change row is 8 bytes:
`relative version u32 | offset u16 | length u16`, in the same newest-first order.
These uncompressed rows permit binary search before a frame is decompressed.

The cumulative leaf index is sorted by leaf. It starts with one `leaf u32` key per
row, so lookups binary-search a dense 4-byte column, followed by 64-byte rows:
`leaf u32 | frame count u32 | directory offset u64 | latest changed epoch u64 |
previous changed epoch u64 | dense list offset u64 | record count u64 |
base epoch u64 | undo charge u32 | zero u32`. The base epoch is the newest epoch at
or before the latest change whose final run ends in a base copy (zero if none);
the undo charge counts work since that base and is below 16384, and zero when the
base is the latest change. Inherited rows retain only leaf, latest changed epoch,
base epoch and undo charge; all other fields are zero.
All offsets are absolute file offsets. Root rows are
`root [32] | version u64`, 40 bytes, sorted by root and then ascending version.
There is one root per version from checkpoint through end inclusive. The file ends
after the roots. An epoch span cannot exceed `2^32-1` versions; sealing returns a
clear error if EpochVersions permits a larger span. Parsers validate bounds,
ordering, dense records, frame directories, flags, and decompression limits.

### Export image

Header, 64 bytes: `PGIMAGE2 [8] | MaxLeaves u64 | version u64 |
used leaf count u64 | root [32]`. Records are `leaf index u64 | bytes [1024]`,
strictly increasing by index. All-zero leaves are omitted, including unused state.
An index of `0xffffffffffffffff`, with no leaf bytes, terminates the image. There
must be no trailing bytes. Leaf zero contains the state header. Import checks its
capacity and size fields and recomputes the complete sparse tree root.

## Verification and measurements

Run `go vet ./...` and `go test -race -count=1 ./...`. CI runs both on
ubuntu-latest and macos-latest with Go 1.26, then each native parser fuzzer for 30s.
For longer fuzz runs:

```
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzLogRecords$' -fuzztime=2m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzBlockRecords$' -fuzztime=2m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzEpochFrame$' -fuzztime=2m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzEpochIndex$' -fuzztime=2m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzExportImage$' -fuzztime=2m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzRewindIntent$' -fuzztime=2m .
```

### Benchmark environment and commands

Measured on 2026-10-14 JST with Go 1.26.0. macOS 27.0.1 runs on Apple M3 with
16384-byte OS pages. Both Ubuntu 24.04 machines use 4096-byte pages and kernel
7.0.14-orbstack-00380-ga7e0a2dc9535. The amd64 machine uses Rosetta and
`GODEBUG=asyncpreemptoff=1`; the compiler crashed under translation without that
setting. These are shared-host measurements with concurrent verification work,
not isolated hardware comparisons. Every range below contains three runs.
Raw output and complete platform command prefixes are in [evidence](evidence/commands.md).

```
go test -run '^$' -bench '^(BenchmarkWrite64|BenchmarkCommit50k|BenchmarkViewReadWindow|BenchmarkViewReadSealed|BenchmarkTreePath|BenchmarkDurabilityLoop|BenchmarkExport)$' -benchtime=1s -count=3 -timeout=30m .
go test -run '^$' -bench '^BenchmarkTreeColdPath$' -benchtime=100x -count=3 .
go test -run '^$' -bench '^(BenchmarkOpenVerified|BenchmarkViewMapFirstTouch)$' -benchtime=10x -count=3 .
```

The cold-path table uses separate 100x runs on every platform.
The cold-path test advises away mapping pages; it does not evict the filesystem cache.

| Measurement | macOS arm64 | Linux arm64 | Linux amd64 (Rosetta) |
|---|---:|---:|---:|
| Write64, us | 0.2523-0.4207 | 0.7969-0.9391 | 0.4999-0.5463 |
| Commit50k, ms | 22.29-29.37 | 37.44-49.12 | 115.1-130.5 |
| Window read, us | 0.571-0.7129 | 0.4244-0.8696 | 0.4952-0.5097 |
| Sealed read, us | 13.53-36.43 | 7.318-7.459 | 8.724-9.312 |
| Export, MB/s | 451.5-483 | 335.2-481.8 | 151.4-153.6 |
| Hot heap path, ns | 89.36-103.7 | 157.5-201.9 | 224.3-236.9 |
| Hot tiled path, ns | 279.1-293.1 | 543.5-776.7 | 654.3-720.9 |
| Advised-cold heap path, us | 48.87-67.79 | 652.5-1017 | 918.8-1208 |
| Advised-cold tiled path, us | 25.93-31.75 | 258.3-513.2 | 352.7-448.4 |
| 64 writes + commit, NoSync ms | 0.07554-0.09183 | 0.06183-0.07064 | 0.3657-0.3662 |
| 64 writes + commit, Sync ms | 204.6-306.3 | 92.06-132.9 | 37.07-52.36 |
| Verified Open, NoSync ms | 21.69-23.29 | 28.85-50.66 | 41.96-48.96 |
| Verified Open, Sync ms | 57.98-61.67 | 91.53-152.5 | 89.6-91.73 |
| View.Map first touch, us | 13.31-24.56 | 17.65-41.5 | 26.62-35.74 |
| View.Map setup, ms | 1.189-1.633 | 0.05484-0.368 | 0.08926-0.1422 |

DurabilityLoop writes 64 bytes to 64 leaves per commit. Sync includes a durable
undo barrier for each write. OpenVerified has 4 MiB of nonzero data and 8 MiB of
allocated state; only Open is timed, with Close excluded. Map setup and first-touch
cost are separate. The first-touch benchmark reads and privately writes one untouched OS page at
32 MiB inside a 64 MiB region.

The focused tree-only comparison used
`go test -run '^$' -bench '^BenchmarkCommit50k$' -benchtime=1x -count=3`:
heap layout 41.23-49.81 ms, tiled layout 33.79-40.17 ms. Its advised-cold path
comparison (`-bench '^BenchmarkTreeColdPath$' -benchtime=100x -count=3`) was
69.93-102.67 us versus 25.24-27.66 us. These early layout-only results precede
the final durability and format changes. Tiling costs extra hot address arithmetic,
but reduces cold mapping-page touches. TestTreeRootMatchesHeap verifies identical roots.

The v1 RAM-index sealed-read baseline was 5.895-7.362 us for three 200ms runs
(`-bench '^BenchmarkViewReadSealed$' -benchtime=200ms -count=3`); a quiet single
run was 2.435 us. The final disk-index results above are slower. Bounded index RAM
and backward reconstruction have a measurable read cost; this is not a latency win.

### Backward XOR compression and reads

```
PAGERDB_XOR_METRICS=1 go test -run '^TestXORWorkloadMetrics$' -count=3 -v .
go test -run '^$' -bench '^BenchmarkXORSealedRead$' -benchtime=100x -count=3 .
```

Each workload has 160016 application writes over 16 leaves and 10001 versions,
with 1616 frames. The EVM-shaped workload uses two adjacent 32-byte uint256 values
with leading zeros, balance-like increments and counters. Random uses 64 random
bytes per write. All three platforms produced the same byte counts in all three runs.
The real application trace was not needed for this synthetic shape test.

| Workload | Before trim, raw B/record | After trim, raw B/record | Before trim, zstd policy B/record | After trim, zstd policy B/record | Frames compressed | Whole epoch B/application record |
|---|---:|---:|---:|---:|---:|---:|
| EVM-shaped | 72.3232 | 41.3567 | 4.0365 | 3.9545 | 100% | 15.3710 |
| Random | 72.3232 | 72.3151 | 72.3232 | 72.3151 | 0% | 83.7315 |

Frame figures include frame headers. Whole-epoch figures also include dense rows,
frame directories, cumulative indexes, roots and allocator metadata. Compression
is kept only when it saves at least 10%. Range-read cost includes index lookup and
decoding. The following direct sealed-reader measurements prevent near-head ages
from selecting the live-log path; public View.Read is checked separately against
captured expected bytes.

| Measurement | macOS arm64 | Linux arm64 | Linux amd64 (Rosetta) |
|---|---:|---:|---:|
| EVM, 1 versions back, us | 1.288-1.492 | 0.73-0.8421 | 1.114-1.189 |
| EVM, 100 versions back, us | 11.47-12.07 | 9.567-10.3 | 11.89-13.7 |
| EVM, 10000 versions back, us | 1545-1634 | 1087-1380 | 1218-1232 |
| Random, 1 versions back, us | 0.8196-0.9288 | 0.5504-0.6079 | 0.7442-0.8292 |
| Random, 100 versions back, us | 6.091-6.556 | 5.432-6.586 | 10.24-10.95 |
| Random, 10000 versions back, us | 1631-1781 | 760.5-933.1 | 1081-1244 |

### Sustained writes with background sealing

`go test -run '^$' -bench '^BenchmarkSustained$' -benchtime=10000x -count=3 -timeout=45m .`
writes 64 bytes into 2400 leaves per block, for 10000 blocks per run. Sealing runs
every 100 versions. Throughput includes draining the sealer; commit percentiles
exclude writes and final drain, but include rotation backpressure. The combined
active and queued epoch count is limited to two for new history.

| Measurement | macOS arm64 | Linux arm64 | Linux amd64 (Rosetta) |
|---|---:|---:|---:|
| Sustained blocks/s | 105.5-180.9 | 80.18-151.9 | 44.08-84.33 |
| Commit p50, ms | 0.6723-0.8139 | 1.271-1.528 | 7.288-9.595 |
| Commit p99, ms | 9.71-13.76 | 9.212-19.46 | 18.77-35.42 |

### Memory and sparse mappings

`go test -run '^TestSealMemoryMeasure$' -count=3 -v` samples extra Go heap every
millisecond around sealing. It excludes preexisting live-log indexes, mapped log
bytes, and OS page cache. Fixed external-sort and frame buffers bound live payload
memory; scratch disk grows with the epoch. Peak heap also includes uncollected
runtime garbage. Broad-leaf memory stays near 2.6-2.7 MB when input grows eightfold.
Same-leaf workloads activate larger zstd tables. Raw outputs are in
`evidence/xor-{macos,linux-arm64,linux-amd64}-memory-final.txt`.

| Measurement | macOS arm64 | Linux arm64 | Linux amd64 (Rosetta) |
|---|---:|---:|---:|
| Broad 1 MiB peak extra heap, B | 2721936-2724488 | 2588880-2592400 | 2589704-2590480 |
| Broad 8 MiB peak extra heap, B | 2723832-2723944 | 2600472-2600848 | 2590312-2600792 |
| Broad 1 MiB total allocation, B | 2723704-2724504 | 2592392-2600936 | 2592616-2601080 |
| Broad 8 MiB total allocation, B | 2723872-2723968 | 2600560-2600904 | 2600792-2600936 |
| Same-leaf 1 MiB peak extra heap, B | 10184640-10827512 | 10826968-11104840 | 10827032-10835960 |
| Same-leaf 8 MiB peak extra heap, B | 10829088-10829664 | 10828912-11189008 | 10632224-10828912 |

`PAGERDB_RSS_TEST=1 go test -run '^TestViewMapSparseRSS$' -count=3 -v` maps a
4 GiB region and touches 16 OS pages. RSS increases were 294912-376832 bytes on
macOS, 106496-319488 on Linux arm64, and 225280-405504 on Linux amd64. The Linux
machines used native userfaultfd with vm.unprivileged_userfaultfd=1.

### Parser verification

All six two-minute fuzz runs passed. The final corpus totals, including cached
interesting inputs, were: log 27 (4507533 executions), block 26 (4453766), XOR frame
297 (7268360), XOR index 19 (10305021), export image 12 (5049573), and rewind intent
5 (7256424). Valid and malformed seed files are committed under testdata/fuzz;
these totals are not counts of committed seed files. The XOR seed set includes a
compressed frame with a closing leaf copy and a complete dense index.

The full suites include crafted lost/torn-log states, kill -9 recovery, rewind
across sealed epochs, export/import, mapped reads, concurrent GC and kernel I/O.
Physical hardware power-cut testing and hosted GitHub Actions execution are
UNVERIFIED. CI is configured but was not published or run remotely.

## Limits

- Version 1 files require migration; they are not opened as version 2.
- Rewind is limited by configured retention. Sealed history remains available for reads.
- Cumulative sealed indexes trade extra disk space for logarithmic lookup per changed epoch.
- Sync costs one barrier per Commit, plus one per Touch call and per DB.Map call with staged writes. On macOS the barrier uses F_FULLFSYNC.
- An unclean Open and DB.Verify hash the whole allocated state, so their cost grows with state size. A clean Open checksums only the retained log chunks.
- Without filesystem cloning, private mapping setup copies sparse file extents.
- Linux userfaultfd needs cgo and a native fault thread. Map setup reconstructs changed historical leaves into a snapshot file. A Go fault handler cannot safely serve arbitrary Go slice access during garbage collection.
- A userfaultfd read failure is fatal rather than supplying incorrect page bytes.
- File corruption outside the durability model is not repaired automatically. Sealed reads are not individually authenticated by a Merkle proof.

## License

Ecosystem License v1.1. See LICENSE.md.
