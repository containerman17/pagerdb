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
would invalidate their versions. Each Commit prunes the in-memory window index to
the versions that rewind retention, open views and pending sealing still need. An
index entry is 16 bytes per log record plus a 12-byte per-leaf link, with no Go
pointers. Entries and links live in fixed pages of 65536, so appending never copies
the window and pruning recycles whole pages. The newest link per leaf sits in a
dense table of 512 KiB pages, one page per 65536 leaves, allocated when a leaf in
its range is first written. Log chunk files stay until they are sealed, the synced state covers them,
their versions leave retention and no view needs them. A version that left the
index before its epoch was sealed returns ErrWindow until sealing finishes.

A **write** stores the old and new bytes in the mapped log, then the new bytes in
the mapped state. It allocates no Go objects. Each log chunk reserves address space
for a whole epoch and its file grows in 64 MiB steps, so the mapping moves only
when one epoch outgrows `EpochBytes` by more than a step. A background goroutine
touches log pages about 8 MiB ahead of the tail, so first-touch page faults happen
off the write path.

**Sealing** runs in the background after `EpochVersions` (default 10000) or
`EpochBytes` (default 1 GiB). Rotation clones the state file with the native
filesystem clone operation, so the sealer reads epoch-end leaves without the
database lock. Without cloning it reconstructs them from the window index in
batches of 64 leaves per lock hold. The sealer scans the mapped log once to count
references per leaf range, partitions contiguous leaf ranges across up to
GOMAXPROCS workers, and routes 40-byte references into per-worker buffers in one
pass. A worker whose range fits in one 32768-record run radix-sorts it in memory;
a larger range rescans the log for its leaves, spills sorted runs to scratch files
and merges them two ways. Each worker writes its section through large buffers. Frames, indexes and roots stream through disk
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

Write and Commit never fsync. A crash recovers the last complete version, or Open
fails loudly; a validator then rewinds to a durable version or resyncs and
re-executes. Commit appends the block record to the mapped log and rotation clones
the state for the sealer. The only per-epoch I/O on the commit path is a writeback
of the mapped state (msync, no fsync) so that the clone sees mapped writes. In the
background, after an epoch is sealed, the sealer writes back and fsyncs the state
file and only then lets old log chunks go. The kept log chunks therefore always
reach back to a checkpoint that the synced state file covers.

An unclean open redoes the new bytes of every kept chunk, oldest first, then undoes
the torn tail of the last chunk with its old bytes, rebuilds the tree and requires
the root of the last complete block. Each chunk's checkpoint must match the previous
chunk's last block, or Open reports a log gap. The kernel may write back any state
or log page at any time, so a state page can reach disk without the log page that
describes it. That case is detected (the rebuilt root differs) and not repaired.
Lost commits are possible: a crash can lose blocks whose log pages were not written
back.

A clean-shutdown marker is removed and its removal synced before opening the
database for use. Close syncs the files before creating the marker. The marker
records the closed version and root with a checksum. A clean open checks the marker,
the log head and the stored tree root against each other and then trusts the stored
tree. Any mismatch, a damaged marker or a missing marker selects the unclean path.
`DB.Verify` rehashes the whole state on demand for operators. Rewind and Close sync
the state, tree and log before they return. Sealed epoch files are fsynced by the
local store.

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

Options are `MaxLeaves`, `RetainVersions`, `EpochVersions`, `EpochBytes`, and
`Store`. Zero values select the defaults described above.
Head range violations panic like slice bounds violations. Proof and Verify reject pending
uncommitted writes. DB.Map slices are invalidated by Free/Grow or allocation rollback.
View.Map snapshots remain valid until View.Close. Mutation methods without an error
return can panic on storage failures, including log growth.
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
with no alignment gaps. An undo entry tag is written after its header and old bytes,
before the state store. Commit fills its reserved new bytes and then writes the
block checksum and commit tag.

```
checkpoint, 56 bytes:
  0 'C', 1..7 padding, 8 version u64, 16 root [32],
  48 CRC-32C of bytes 8..47 u32, 52..55 padding
entry, 32+2*n bytes:
  0 'U', 1..7 reserved, 8 n u32,
  12 region u32, 16 version u64, 24 state offset u64, 32 old [n],
  32+n new [n]
commit, 56 bytes:
  0 'B', 1..3 padding, 4 entry count u32, 8 version u64,
  16 root [32], 48 CRC-32C u32, 52..55 padding
```

CRC-32C uses the Castagnoli polynomial. Undo entries have no per-entry checksum.
The reader also accepts legacy E entries with their undo checksum at byte 4.
The commit checksum covers all complete entry
bytes since the preceding checkpoint or commit, then commit bytes 4..47. A chunk starts with one checkpoint. Unused tail
bytes are zero. The current chunk's file grows in 64 MiB steps; sealing truncates
it after the last record. A tag alone is not a durability barrier.

A process crash leaves the shared page cache intact. Recovery undoes raw entries
after the last complete block, in reverse order. Write publishes each undo entry
before changing state. Commit copies final new bytes in parallel and builds the
window index. A view or Undo can build that index earlier if needed. A fixed-size
cache skips repeated exact ranges until Mark, Undo, or Commit.
Cache collisions only reduce deduplication. Overlapping ranges retain their undo
images; Mark starts a new cache generation so Undo restores the marked state.
The new-byte space is reserved during Write but is not copied until Commit.

There is no writeback ordering guarantee across an OS crash or power loss. A block
checksum detects torn committed blocks; recovery checks the reconstructed root.
If recovery cannot reconstruct that root, Open fails and the node must resync.
Commit does not promise durability when it returns.

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
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzLogRecords$' -fuzztime=1m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzBlockRecords$' -fuzztime=1m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzEpochFrame$' -fuzztime=1m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzEpochIndex$' -fuzztime=1m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzExportImage$' -fuzztime=1m .
GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzRewindIntent$' -fuzztime=1m .
```

### Benchmark environment and commands

Measured on 2026-10-14 JST with Go 1.26.0 at commit 60d5083. macOS 27.0.1 runs on an
Apple M3 (16 GB) with 16384-byte OS pages. Linux arm64 is an OrbStack virtual
machine on the same M3 (Ubuntu 24.04, kernel 7.0.14-orbstack, 4096-byte pages,
7 CPUs). Linux amd64 under Rosetta ran the correctness suites only; its timings
measure the translator, not the code. The host was shared: every timed run first
waited until no other benchmark process used more than 20% CPU. Every range below
contains three runs. Raw output and complete command prefixes are in
[evidence](evidence/commands.md).

```
go test -run '^$' -bench '^(BenchmarkWrite64|BenchmarkCommit50k|BenchmarkViewReadWindow|BenchmarkViewReadSealed|BenchmarkTreePath|BenchmarkDurabilityLoop|BenchmarkExport)$' -benchtime=1s -count=3 -timeout=30m .
go test -run '^$' -bench '^BenchmarkTreeColdPath$' -benchtime=100x -count=3 .
go test -run '^$' -bench '^(BenchmarkOpenVerified|BenchmarkViewMapFirstTouch)$' -benchtime=10x -count=3 .
```

| Measurement | macOS arm64 | Linux arm64 |
|---|---:|---:|
| Write64 (64 bytes into one leaf), ns | 146.9-151.0 | 210.1-252.4 |
| Commit50k (50000 dirty leaves), ms | 12.09-12.38 | 15.18-17.33 |
| Window read (View.Read inside retention), ns | 171.9-197.8 | 204.3-225.5 |
| Sealed read (View.Read of a sealed version), us | 1.142-1.178 | 1.262-1.329 |
| Export, MB/s | 1958-1960 | 1921-1954 |
| Random heap path, ns | 63.70-68.03 | 83.60-90.53 |
| Random tiled path, ns | 94.18-95.69 | 108.4-116.4 |
| Hot heap path, ns | 12.49-12.62 | 29.72-32.50 |
| Hot tiled path, ns | 19.35-19.43 | 19.47-19.54 |
| Advised-cold heap path, us | 39.30-41.78 | 720.6-848.1 |
| Advised-cold tiled path, us | 10.07-12.89 | 218.8-302.7 |
| 64 writes + commit, us | 34.13-34.96 | 35.02-36.07 |
| Clean Open, ms | 5.604-5.959 | 2.752-3.556 |
| Unclean Open, ms | 16.83-18.56 | 21.87-23.85 |
| DB.Verify, ms | 8.806-9.221 | 6.473-11.75 |
| View.Map first touch, us | 12.11-17.06 | 14.09-34.08 |
| View.Map setup, ms | 1.060-1.293 | 0.0467-0.0693 |

DurabilityLoop writes 64 bytes to 64 leaves per commit. OpenVerified has 4 MiB of
nonzero data and 8 MiB of allocated state; only Open (or Verify) is timed. A clean
Open trusts the checkpointed root; an unclean Open redoes the log and rehashes the
state. The cold-path test advises away mapping pages; it does not evict the
filesystem cache. The first-touch benchmark reads and privately writes one
untouched OS page at 32 MiB inside a 64 MiB region; map setup is reported
separately. TestTreeRootMatchesHeap verifies that the tiled and heap layouts give
identical roots.

Version 1 on the same M3, as measured before this release and not rerun here:
Write64 207-240 ns, Commit50k 12.4-14.5 ms, window read 175-198 ns, sealed read
1.45-1.6 us. Version 2 is faster on writes and sealed reads, and equal on commits and
window reads, while it keeps sealed history on disk instead of in RAM.

### Write path

```
go test -run '^$' -bench '^BenchmarkWrite32Resident$' -benchtime=5000000x -count=3 .
PAGERDB_EVM_RETAIN=32 go test -run '^$' -bench '^BenchmarkEVM$' -benchtime=200x -count=3 .
```

BenchmarkWrite32Resident reads or writes 32 bytes at random 64-byte slots of a
128 MiB region, with an untimed Commit every 50000 operations. Each random slot
misses the CPU caches, so these figures are mostly memory latency. Before is
commit 0d4b417, which checksummed each log entry in three passes.

| Operation, ns | macOS before | macOS after | Linux arm64 after |
|---|---:|---:|---:|
| Write | 120.4-123.1 | 114.9-119.9 | 148.6-158.8 |
| Read | 38.96-40.86 | 38.42-46.45 | 44.56-54.55 |
| Read, then write the same slot | 134.8-139.7 | 127.7-129.8 | 162.0-182.6 |

BenchmarkEVM executes 200 blocks of 50000 read-increment-write operations on
32-byte values in 2,000,000 slots of 64 bytes (128 MiB), with RetainVersions 32.
Slots are uniform or normally distributed around a hot spot (sigma in slots).
Write and commit times are per block.

| Measurement | uniform | sigma 20000 | sigma 2000 |
|---|---:|---:|---:|
| macOS writes/s, millions | 1.753-1.841 | 3.420-3.665 | 3.811-3.989 |
| macOS p50 write, ms | 6.800-7.100 | 2.760-2.774 | 2.274-2.284 |
| macOS p99 write, ms | 7.499-8.205 | 3.307-3.405 | 2.402-2.469 |
| macOS p50 commit, ms | 6.797-6.858 | 1.423-1.433 | 0.6669-0.6713 |
| macOS p99 commit, ms | 8.904-9.094 | 1.712-1.806 | 0.7405-0.7517 |
| macOS before, p50 write, ms | 7.207-7.309 | 3.258-3.330 | 2.749-2.762 |
| Linux arm64 writes/s, millions | 1.590-1.683 | 2.744-2.927 | 2.948-3.441 |
| Linux arm64 p50 write, ms | 8.534-8.685 | 2.977-3.127 | 2.388-2.486 |
| Linux arm64 p99 write, ms | 10.87-32.70 | 7.369-10.94 | 5.353-6.676 |
| Linux arm64 p50 commit, ms | 7.603-7.952 | 2.234-2.275 | 0.8999-0.9563 |
| Dirty leaves per block | 41214 | 6358 | 836 |

At sigma 2000 a block of writes takes 2.27 ms on macOS, about 45 ns per
read-increment-write including the benchmark's random sampling. The uniform case
is bound by cache and TLB misses on random state lines. A write allocates no Go
objects of its own; index pages of 65536 entries are allocated or recycled as the
window moves. Writes are serial by design: one goroutine owns mutations, and only
Commit hashing and sealing use more cores. Process RSS was 1.30-1.43 GB on both
platforms, mostly the mapped state, log and tree.

### Window index memory

```
go test -run '^$' -bench '^BenchmarkWindowIndexMemory$' -benchtime=1x -count=3 .
```

The benchmark commits blocks of 20000 random 32-byte writes into a 64 MiB region
and reports Go heap per retained write after a forced GC; log bytes live in mapped
files and are not counted. With the default RetainVersions 4096, 100 blocks all
stay in the window: 27.83-27.90 bytes per write (55.7 MB). With RetainVersions 32,
300 blocks run inside one epoch and the window keeps 32: 32.54-32.56 bytes per
write (20.8 MB). Both platforms match. At commit c77f77c the index cost 81.3 and
81.9 bytes per write, and RetainVersions 32 still held 300 blocks (491.5 MB) because
pruning followed log chunks instead of versions.

### Backward XOR compression and reads

```
PAGERDB_XOR_METRICS=1 go test -run '^TestXORWorkloadMetrics$' -count=3 -v .
go test -run '^$' -bench '^BenchmarkXORSealedRead$' -benchtime=100x -count=3 .
```

Each workload has 160016 application writes over 16 leaves and 10001 versions,
with 1616 frames. The EVM-shaped workload uses two adjacent 32-byte uint256 values
with leading zeros, balance-like increments and counters. Random uses 64 random
bytes per write. Both platforms produced the same byte counts in all three runs.

| Workload | Before trim, raw B/record | After trim, raw B/record | Before trim, zstd policy B/record | After trim, zstd policy B/record | Frames compressed | Whole epoch B/application record |
|---|---:|---:|---:|---:|---:|---:|
| EVM-shaped | 72.3232 | 41.3567 | 4.0365 | 3.9987 | 100% | 15.6676 |
| Random | 72.3232 | 72.3151 | 72.3232 | 70.7029 | 32.67% | 82.3718 |

Frame figures include frame headers. Whole-epoch figures also include dense rows,
frame directories, cumulative indexes, roots and allocator metadata. Compression
is kept per frame only when it saves at least 10%. The direct sealed-reader
benchmark reads 64 bytes and bypasses the live log, so near-head ages still decode
sealed history; public View.Read is checked separately against captured bytes.
EVM and Random seal every 100 versions, so 10000 versions back crosses 100 epochs;
the Epoch10000 variants use the default epoch length.

| Read, us | macOS arm64 | Linux arm64 |
|---|---:|---:|
| EVM, 1 version back | 0.4154-0.4621 | 0.4858-0.5546 |
| EVM, 100 versions back | 9.135-9.405 | 9.440-9.528 |
| EVM, 10000 versions back | 35.09-35.51 | 35.95-36.49 |
| Random, 1 version back | 0.2367-0.4092 | 0.2471-0.2892 |
| Random, 100 versions back | 3.730-4.381 | 3.760-3.907 |
| Random, 10000 versions back | 15.70-16.85 | 15.45-15.60 |
| EVM, 10000-version epochs, 1 back | 0.4233-0.4508 | 0.4279-0.5833 |
| EVM, 10000-version epochs, 100 back | 19.16-20.00 | 19.17-21.40 |
| EVM, 10000-version epochs, 10000 back | 20.39-20.72 | 20.17-20.93 |
| Random, 10000-version epochs, 1 back | 0.2371-0.2750 | 0.2467-0.2721 |
| Random, 10000-version epochs, 100 back | 9.969-10.80 | 9.932-11.59 |
| Random, 10000-version epochs, 10000 back | 8.035-8.305 | 8.182-8.627 |

A deep read is bounded by its base copies, not by its age. Each leaf gets a base
copy whenever about 16 KiB of history has accumulated since the last one, so a
read takes at most a few base hops (two index lookups each), then walks at most
the changed epochs since that copy with one O(log leaves) index lookup per epoch,
and decodes at most 16 KiB plus two frames. The EVM read 10000 versions back walks
up to 100 changed epochs and stays near 36 us.

### Sustained writes with background sealing

```
go test -run '^$' -bench '^(BenchmarkSustained|BenchmarkWriteCommitUntimedSeal|BenchmarkWriteCommitOnly)$' -benchtime=10000x -count=3 -timeout=60m .
```

Each block writes 64 bytes into each of 2400 leaves, so 2400 leaves of 1 KiB are
dirty, then commits; a run is 10000 blocks. Sustained seals every 100 versions in the background and includes the final
drain. WriteCommitUntimedSeal runs the same loop and excludes the time Seal takes
from its blocks/s, so it is the write and commit cost with sealing removed.
WriteCommitOnly never seals, so its single log chunk grows to about 3.8 GB. Its
macOS runs varied threefold, so it is not a fair baseline there.

| Measurement | macOS arm64 | Linux arm64 |
|---|---:|---:|
| Sustained, blocks/s | 1635-1717 | 851.3-919.8 |
| Write and commit without sealing, blocks/s | 2129-2204 | 1084-1152 |
| Write and commit, never sealing, blocks/s | 347.6-1039 | 1076-1152 |
| Sustained commit p50, ms | 0.3628-0.3764 | 0.7364-0.7624 |
| Sustained commit p99, ms | 2.029-2.197 | 4.240-4.538 |
| Backpressure events per run | 0 | 0-1 |
| Final drain, ms | 94.52-101.6 | 45.43-153.2 |

Sustained throughput is 74-81% of write and commit without sealing on macOS and
74-85% on Linux arm64. The gap is CPU shared with the sealer: write and commit
times rise while it runs. Rotation waited for the sealer once in three Linux runs.

### Memory and sparse mappings

`go test -run '^TestSealMemoryMeasure$' -count=3 -v` samples extra Go heap every
millisecond around sealing. It excludes the live window index, mapped log bytes,
and OS page cache. Sealing memory depends on the number of workers, not on the
epoch size: each of up to GOMAXPROCS workers keeps about 5.4 MB of sort, frame and
compression buffers.

| Peak extra heap during Seal, MB | macOS arm64 (8 workers) | Linux arm64 (7 workers) |
|---|---:|---:|
| Same leaf, 1 MiB of writes | 5.74-6.74 | 6.63-6.66 |
| Same leaf, 8 MiB of writes | 7.24-7.28 | 5.73-7.19 |
| Broad leaves, 1 MiB of writes | 12.21 | 11.78-12.04 |
| Broad leaves, 8 MiB of writes | 43.28-43.29 | 35.87-37.59 |

A separate macOS run with 8192 to 262144 broad writes (`v2final-macos-memory-plateau.txt`)
stayed at 43.3-44.7 MB from 32768 writes up, and same-leaf runs at 5.9-7.3 MB.

`PAGERDB_RSS_TEST=1 go test -run '^TestViewMapSparseRSS$' -count=3 -v` maps a
4 GiB region and touches 16 OS pages. RSS increases were 294912-425984 bytes on
macOS and 90112-356352 bytes on Linux arm64. The Linux machine used native
userfaultfd with vm.unprivileged_userfaultfd=1.

### Parser verification

All six one-minute fuzz runs on macOS passed at commit 60d5083. Executions and final
corpus totals, including cached interesting inputs: log 5332430 (31), block 5302721
(28), XOR frame 4565106 (705), XOR index 5781358 (42), export image 5406534 (14),
rewind intent 5350027 (5). Valid and malformed seed files are committed under
testdata/fuzz. The XOR seed set includes a compressed frame with a closing leaf copy
and a complete dense index.

The full suites passed on macOS arm64, Linux arm64 and Linux amd64 (Rosetta): go vet,
go test and go test -race. They include crafted lost and torn log states, kill -9
recovery, rewind across sealed epochs, export and import, mapped reads, concurrent GC
and kernel I/O. Physical power-cut testing and hosted GitHub Actions execution are
UNVERIFIED. CI is configured but was not published or run remotely.

## Limits

- Version 1 files require migration; they are not opened as version 2.
- Rewind is limited by configured retention. Sealed history remains available for reads.
- Cumulative sealed indexes trade extra disk space for logarithmic lookup per changed epoch.
- Commits are not durable when Commit returns. A crash can lose recent blocks or, if a state page reached disk before its log page, make Open fail; the caller then restores or re-executes.
- An unclean Open redoes every kept log chunk, so its cost also grows with the retained log.
- Versions older than RetainVersions in an epoch that is not sealed yet return ErrWindow until the sealer finishes that epoch. Rotation writes the mapped state back (msync without fsync) once per epoch so the clone sees it.
- The window index holds at most 2^32 log records and log chunks up to 1 TiB; larger windows panic and need a smaller RetainVersions or EpochBytes.
- Writes are serial. One goroutine owns mutations and every Write takes the database mutex; only Commit hashing and sealing use several cores. Random writes over a state larger than the CPU caches cost about 115-160 ns each on the measured M3.
- Sealing holds about 5.4 MB of buffers per worker, up to GOMAXPROCS workers, independent of epoch size.
- Each live log chunk reserves address space for one epoch plus 64 MiB (at most 4 GiB plus 64 MiB); the file itself grows in 64 MiB steps.
- An unclean Open and DB.Verify hash the whole allocated state, so their cost grows with state size. A clean Open checksums only the retained log chunks.
- Without filesystem cloning, private mapping setup copies sparse file extents.
- Linux userfaultfd needs cgo and a native fault thread. Map setup reconstructs changed historical leaves into a snapshot file. A Go fault handler cannot safely serve arbitrary Go slice access during garbage collection.
- A userfaultfd read failure is fatal rather than supplying incorrect page bytes.
- File corruption outside the durability model is not repaired automatically. Sealed reads are not individually authenticated by a Merkle proof.

## License

Ecosystem License v1.1. See LICENSE.md.
