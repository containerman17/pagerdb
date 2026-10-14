# pagerdb

A page store in pure Go. PagerDB keeps regions of bytes in one mmap'd file, a SHA-256
binary tree over 1 KiB leaves of that file, a live log of every write, and sealed epoch
files for history. It knows pages, versions and roots. It knows nothing about blocks,
transactions, accounts or execution engines.

Dependencies: the standard library and `golang.org/x/sys/unix`. Runs on Linux and macOS
(the OS page size is a host property, never a format constant; tested on macOS arm64 with
16 KiB pages, compiled for Linux amd64).

## Design

**Leaves and the tree.** The leaf is 1 KiB, the leaf index is the state file offset / 1 KiB.
Leaf hash = sha256(leaf), node = sha256(left || right). The tree is a heap-layout array of
32 B nodes (node 1 is the root, node i has children 2i and 2i+1) in its own mmap'd file
`tree`, sized at creation for `MaxLeaves` (a power of two, default 2^24 = 16 GiB of state).
An all-zero slot means "empty subtree": the hash of an empty subtree of each level is
precomputed, so the tree file stays sparse. The header of the state file is hashed like any
other leaf, so the root covers the allocation table as well. A later layout optimization is
to store 7-level subtrees per OS page instead of whole levels, so one path touches about 4
pages instead of 24; it changes only the tree file, not the root.

**Regions.** `Alloc(size)` returns a stable `Region` id (uint32). Sizes up to 1 MiB round up
to a class (1, 2, 4, ... 1024 KiB) carved from 2 MiB segments, one class per segment.
Bigger regions are a chain of whole 2 MiB segments. `Grow` within the classes copies into a
slot of the next class and frees the old one (the id stays); a region above 2 MiB grows by
appending segments, without a copy. Freed slots go to a per-class free list linked through
the slots themselves, freed segments to a free segment chain, freed ids to an id stack; all
heads live in the state file header. Freed space is zeroed. `Map` returns one contiguous
slice: a slice of the shared file mapping for slots, and for chains a PROT_NONE anonymous
reservation with every segment mapped over it with MAP_FIXED|MAP_SHARED.

Every allocator change is a logged write to the header, so Undo, Rewind, recovery and views
treat allocation exactly like data. The root depends only on region contents and on the
allocation sequence.

**Writes.** `Write` appends an entry (old bytes and new bytes) to the live log, then stores
the bytes and marks the leaves dirty. `Touch` is the path for code that writes directly
through `Map`: it logs the old bytes now, `Commit` copies the new bytes into the entry. The
live log is itself mmap'd, so the entry reaches the page cache before the state does, and
its tag byte is written last: after a kill -9 every state change has its old bytes in the
log.

**Commit.** Leaves dirtied since the last Commit are hashed in parallel (GOMAXPROCS
workers), then the path nodes level by level, each node once per block. The block record
(version, root, entry count, CRC-32C of the block's entries) is appended and the version
advances. Commit is the only point where the root changes.

**Epochs.** One live log chunk per epoch, `log/000000000001` and so on. Each chunk starts
with a checkpoint mark (version, root). An epoch closes after `EpochVersions` versions or
`EpochBytes` of log; with `Sync` the closing chunk, the state and the tree are msync'd
before the next checkpoint is written. A background goroutine seals each closed chunk into
an epoch file and hands it to the `EpochStore` (pluggable epoch storage: `Put`, `Open`
returning an `io.ReaderAt` and size, `Latest`); `LocalStore` keeps the files under
`epochs/`. Once an epoch is sealed and no View still needs it, its log chunk is deleted.

**Views.** A View at version v inside the window (the versions still covered by log chunks)
reads head bytes and applies the old bytes of every overlapping entry newer than v, newest
first, so the oldest one wins. A per-leaf chain of entries (newest first) makes the lookup
stop at the first entry at or below v. Region tables and segment chains are read through
the same path, so a region resolves as it was at v. Window views pin their version; a chunk
is not dropped while a view below its end is open. Below the window a View reads sealed
epochs: for each leaf, the first sealed epoch from v's epoch on that changed the leaf holds
its bytes at v (frame base, plus entries up to v when the epoch contains v); if no sealed
epoch changed it, the window has the bytes at the last sealed version. `ViewRoot` looks
roots up in RAM for the window and in each sealed epoch's sorted root table.

**Recovery.** `Open` replays the newest chunk that has a checkpoint: complete blocks forward
with new bytes (idempotent), the entries after the last complete block backwards with old
bytes, then rehashes every leaf the chunk touched and requires the root to equal the last
block record's root. Older chunks not yet sealed are loaded into the window and queued for
sealing; chunks already sealed are deleted. A chunk without a checkpoint (crash during
rotation) is removed.

**Rewind and Undo.** `Undo(mark)` restores the old bytes of every entry since `Mark()` in
the pending block, newest first, and cuts the log. `Rewind(v)` does the same for every entry
newer than v, rehashes, checks the root of v and cuts the log at v.

## File formats

All integers little endian.

State file, segment 0 onward (header segments, then data segments):

| offset | field |
|---|---|
| 0 | magic `PAGERDB1` |
| 8 | MaxLeaves u64 |
| 16 | segments in use u64 (header included) |
| 24 | next never-used region id u64 |
| 32 | free region id stack head u64 |
| 40 | free segment chain head u64 |
| 48 | 11 classes x (free slot head u64, carve offset u64) |
| 1024 | segment chain: next segment u64 per segment |
| aligned 1 KiB | region table: 16 B per id: kind u32 (0 free, 1..11 class, 12 chain), segment count u32, slot offset or first segment u64 |

The header is MaxLeaves/16 region ids large (2^20 regions at the default capacity, 9 of the
8192 segments).

Live log chunk records (the tag byte is written last):

```
entry       'E' _[3] region u32 | length u32 _[4] | version u64 | state offset u64 | old [n] | new [n]
block       'B' _[3] entries u32 | version u64 | root [32] | crc32c u32 _[4]
checkpoint  'C' _[7] | version u64 | root [32]          (first record of every chunk)
```

Epoch file:

```
header (64 B)  "PGEPOCH1" | epoch | start version | end version | changed leaves | index offset | roots | roots offset
frames         per changed leaf, leaves in order, versions in (start, end]:
               flags u8 (compression, 0 = none in v1) _[3] | entry bytes u32 | first version u64 |
               base leaf [1024] (the leaf before the frame's first entry) |
               entries: uvarint version delta | offset in leaf u16 | length u16 | bytes
               a frame holds at most 256 KiB of entries, the next starts with a fresh base
index          per changed leaf, sorted: leaf u32 | frame count u32 | first frame offset u64
leaf hashes    per changed leaf in index order: sha256 of the leaf at the epoch end
roots          (root [32], version u64) for every version in [start, end], sorted by root
```

## API

```go
Open(dir string, o Options) (*DB, error)
(*DB) Close() error
(*DB) Alloc(size int) (Region, error)
(*DB) Grow(r Region, size int) error
(*DB) Free(r Region) error
(*DB) Map(r Region) ([]byte, error)           // valid until Free or Grow of r
(*DB) Read(r Region, off, n int, buf []byte)  // head
(*DB) Write(r Region, off int, b []byte)
(*DB) Touch(r Region, off, n int)             // before writing through Map
(*DB) Mark() Mark
(*DB) Undo(m Mark)
(*DB) Commit() (root [32]byte, version uint64, err error)
(*DB) Root() [32]byte
(*DB) Version() uint64
(*DB) View(v uint64) (*View, error)
(*DB) ViewRoot(root [32]byte) (*View, error)
(*View) Read(r Region, off, n int, buf []byte) error
(*View) Copy(r Region) ([]byte, error)
(*View) Version() uint64
(*View) Close()
(*DB) Rewind(v uint64) error
(*DB) Proof(r Region, off int) (Proof, error)
Verify(root [32]byte, p Proof) bool
(*DB) Seal() error

type Options struct {
	MaxLeaves     uint64     // default 2^24, fixed at creation
	Sync          bool       // msync log at Commit, state and tree at checkpoints
	EpochVersions uint64     // default 10000
	EpochBytes    int64      // default 1 GiB
	Store         EpochStore // default LocalStore under dir/epochs
}
```

One goroutine owns the write API (`Alloc` to `Seal`, `Read`, `Root`, `Version`). Views may
be used from other goroutines (a view reading a range the owner is writing through `Map`
after `Touch` reads those head bytes unsynchronized, then replaces them with the logged old
bytes, so the result is right but the race detector flags it). `Write`, `Read` and `Touch` panic on a range outside the
region, like a slice index.

## Numbers (Apple M3, 8 cores, macOS, 16 KiB pages)

`go test -run '^$' -bench . -benchtime 2s -count 3 .`, default capacity (2^24 leaves). The
machine had other load (load average 5 to 10) during the runs, so ranges are given.

| benchmark | result |
|---|---|
| Write64: 64 B write at a random offset of 256 MiB of regions | 207 to 240 ns/op (one run under load: 400 to 450) |
| Commit50k: Commit of 50,000 distinct dirty leaves out of 1 GiB of regions | 12.4 to 14.5 ms, 3.4M to 4.0M leaves/s (hashing leaves and all paths) |
| ViewReadWindow: View.Read 64 B, 100 versions (100k writes) behind head | 175 to 198 ns/op |
| ViewReadSealed: same read after sealing into epochs of 50 versions | 1,450 to 1,605 ns/op |

The sealed read is three leaf reconstructions (two for the region table and one for the
data), each a binary search in a RAM index and two ReadAt calls on the page-cached epoch
file. Write64 excludes Commit time.

## Not done in v1

- Rewind works only within the current epoch (since the last checkpoint). Crossing into a
  sealed epoch needs a delete in `EpochStore` or lagged sealing.
- Durability: kill -9 is covered by tests. With `Sync`, committed blocks are durable against
  an OS crash, but the kernel may write back state pages of an uncommitted block before
  their log entries, which recovery cannot undo after a power loss. Without `Sync` there is no
  OS crash guarantee.
- Log entries store the state file offset; the region id is informational.
- The epoch file is built in RAM before `Put`; every sealed epoch's index stays in RAM.
- Sealed reads walk forward over epochs when a leaf did not change in v's epoch, one index
  probe per epoch.
- Segments carved for a small class never return to the segment pool.
- No compression (the frame flag byte is there), no 7-level tree pages, no export/import,
  no lazy page faults for old-version `Map`.
- `Proof` refuses while uncommitted writes exist (also after an `Undo` of all of them,
  until the next Commit).
