# PagerDB v3

PagerDB stores byte regions, versions, and SHA-256 roots. Go 1.26, Linux and
macOS. Its only dependency is `golang.org/x/sys/unix`.

```go
db, err := pagerdb.Open(dir, pagerdb.Options{})
if err != nil { return err }
defer db.Close()
r, err := db.Alloc(4096)
if err != nil { return err }
db.Write(r, 0, []byte("hello"))
version, err := db.Commit()
if err != nil { return err }
// Execute the next block here while the previous snapshot hashes.
root, err := db.RootAt(version)
if err != nil { return err }
_ = root
```

## Ownership and API

One goroutine owns **all DB methods and mapped slices**, including reads, root
waits, View creation and Close. There is no mutex in Write or Touch. Internal
workers never read live state. A returned View owns its bytes and can be read
independently of the DB. Do not mutate a View concurrently with its readers.

- `Alloc(size)`, `Grow(r, size)`, `Free(r)` use stable uint32 region handles.
  Map returns the exact requested size. Freed handles are not reused. Grow can
  relocate a region; obtain a fresh Map afterward.
- `Map(r)` returns zero-copy anonymous memory. Call `Touch(r, off, n)` **before
  every direct write**, including repeated and overlapping writes. `Write` does
  this automatically. `Read(r, off, n, dst)` copies bytes. Invalid Write, Touch,
  Read ranges panic; allocation and mapping operations return errors.
- `Mark()` and `Undo(mark)` restore overlapping writes and allocation changes
  inside the current block. Marks expire at Commit or Rewind and after their
  prefix is undone. Grow, Free, allocation rollback, Rewind and Close invalidate
  affected Map slices. Do not use invalidated slices.
- `Commit() (version uint64, err error)` returns after the snapshot copy.
  `RootAt(v) (root [32]byte, err error)` waits for that version's hash.
  `Root()` waits for the head root. `CommitWait() (root, version, err)` combines
  both steps and preserves the old blocking call shape under a new name.
- `Version()` is the submitted head, which may still be hashing. RootAt, View,
  and Rewind support the last `RetainVersions` transitions plus their base.
  Future versions return ErrVersion; expired versions return ErrWindow.
- `View(v)` copies allocated state and replays RAM undo. It is independent of
  later writes, pruning, Rewind and DB.Close. View.Map permits private writes;
  these do not update the View's recorded root. Close a View to release memory.
- `Rewind(v)` requires no pending writes. It restores the retained version,
  discards the replaced suffix, journals a rewind frame and waits as Checkpoint
  does before returning.
  New commits reuse version numbers on the new branch. Existing Views survive.
- `Proof(r, off)` proves the leaf containing that byte at the committed head;
  pending writes are rejected. `Verify(root, proof)` checks the hash path. This
  proves the leaf, not the region-to-leaf mapping. `DB.Verify()` independently
  rebuilds the root from allocated state and checks the head.
- `Flush()` waits until queued blocks are written to the journal. It does not
  fsync and does not wait for the image applier. `Checkpoint()` returns once the
  journal is synced and the image file and CURRENT cover the committed head.
  `Close()` drains the journal queue, stops the applier after its current batch
  and discards pending writes. Unapplied frames replay on the next Open.

## Design

Leaves are **4096 bytes**, a consensus constant. Leaves use SHA-256 over their
bytes; parents use SHA-256(left || right). Empty subtree hashes are precomputed.
Capacity is a fixed power of two. The root includes allocation metadata and all
state bytes, including zero padding. A region allocator uses contiguous,
leaf-aligned, first-fit extents. Allocation scans live regions; allocation is
not the optimized hot path. Free clears bytes before returning the extent.
Metadata reserves 16 bytes per possible region, capped at 1,048,576 entries.
Handle zero is invalid. The header, capacity, and next handle are committed.

State and the sparse virtual binary tree use MAP_PRIVATE | MAP_ANON. Linux
requests MADV_HUGEPAGE; the kernel decides whether to provide huge pages. macOS
uses ordinary anonymous mappings. Tree nodes in untouched virtual pages use
precomputed empty hashes. The writer never touches a file mapping: only the
background applier writes the image file.

Write and Touch append absolute offsets and old bytes to an in-RAM undo buffer,
then set dirty-leaf bits. They do not deduplicate: the caller can use first-write
tracking where its transaction semantics permit it. Mark/Undo need every change
since a mark. The packed buffer has no Go object per write. Capacity grows by
at least doubling and stays with each reusable buffer. The smaller of the last
two block sizes seeds a fresh buffer, avoiding repeated growth during steady
workloads without propagating a single large setup block to every buffer.

Commit waits for the previous hash and a free snapshot slot, then scans dirty
bitmap words in order. It extracts set bits with trailing-zero counts and clears
each scanned word. The scan covers only the range between the first and last
dirty word. No dirty-ID sort is needed. The persistent worker pool copies leaves
into an immutable snapshot, then hashes leaves and folds sorted parent paths
while the owner executes the
next block. At most one hash overlaps one execution. Hash completion publishes
the root independently of journal completion.

Three strongly retained snapshot buffers form a fixed ring. A slot stays immutable
until the disk worker finishes with it, including any checkpoint for that block.
Buffers grow geometrically only when a block exceeds their capacity. Reuse never
clears them; the parallel copy overwrites exactly the required bytes. GC cannot
discard these buffers. Metadata/completion objects, undo buffers, dirty IDs, hash
scratch space, and the journal assembly buffer are also reused. A warmed fixed
workload allocates no heap memory per block, including its hash and journal work;
buffer growth, checkpoints and epoch rotation can allocate.

A separate disk worker appends one journal frame per block with no per-block
fsync. A frame holds redo records only. The worker marks every 32-byte chunk
that the block's undo records touch, then writes each changed leaf once, in leaf
order: the leaf id, a 128-bit chunk mask and the final bytes of the marked
chunks, read from the snapshot. Repeated writes to one chunk cost one chunk. A
rewind frame marks every chunk of every leaf the rewind changed. Old bytes stay
in RAM for Mark, Undo, View and Rewind; the journal does not keep them.

The state image lives in one file, `image`, the size of the state and leaf
aligned. It is sparse until written. A background applier keeps it current:

1. Take the journal end the disk worker has published.
2. fdatasync each journal epoch up to that end (fsync on macOS). Only synced
   frames reach the image, so the image never holds a block the journal lost.
3. Copy each frame's redo into a MAP_SHARED mapping of the image file. A leaf
   rewritten by several blocks in one batch reaches the disk once.
4. Sync the image. On Linux the applier first drops the mapping's page table
   entries (MADV_DONTNEED keeps the data dirty in the page cache), then calls
   fdatasync, so writeback never write-protects mapped pages and never sends
   TLB-flush interrupts to the writer's cores. macOS uses msync. Then write
   `CURRENT.tmp`, fsync it, rename it to CURRENT and
   fsync the directory. CURRENT names the journal position after the last
   applied frame, its version and root.
5. Delete journal epochs that lie entirely before that position.

The applier starts a batch at most once per second, or at once when Checkpoint,
Rewind or the lag bound waits for it. It never blocks Commit directly. An image
I/O error arrives as a fault on the mapping; the applier turns it into the sticky
disk error. Mapped image pages are page cache, not anonymous memory, so the
process holds one anonymous copy of the state.

Backpressure has two bounded stages. The disk queue is bounded by the three
snapshot slots: if all are busy, Commit waits for the oldest disk consumer. The
applier may trail the journal by at most 4 GiB of frames. Past that, the disk
worker waits before its next append, the snapshot slots fill and Commit waits.
Below the bound the writer never waits for the applier. Disk errors become sticky
and surface at the next Commit, Flush, Checkpoint, or Close. A commit already
returned before a disk error may have a valid RAM root but no journal record.
Commit makes no durability promise; Checkpoint does.

## Recovery

Open takes an exclusive process lock and checks CURRENT. It copies every nonzero
image leaf into anonymous state (SEEK_DATA skips holes), then replays the redo of
every journal frame after CURRENT's position, checking each frame's CRC-32C and
that its parent is the previous version. It hashes the loaded and replayed leaves,
checks the root of the last frame (or CURRENT's root when no frame follows) and
checks allocator bounds. Open returns at the last complete frame.

Redo copies are idempotent in order, so a crash at any applier step is safe: the
image may hold frames after CURRENT's position, and the replay writes every byte
those frames changed. The first short or damaged frame ends the chain. Open
truncates that epoch there, removes later epochs, syncs, and starts a new epoch.
Such frames were never applied, because the applier uses only synced frames. A
fresh store writes the genesis header to the image and publishes CURRENT at
version zero. Corrupt image bytes fail the root check; they are not replaced.

A process crash keeps every frame written before it (page cache). An OS crash or
power loss keeps at least the last Checkpoint, Rewind or applier batch, plus any
later frames that reached the disk. The protocol uses file and directory sync
ordering; physical power-cut testing remains unverified. Version 3 stores
(CURRENT magic `PGCUR003`, `checkpoint` file) and v2 stores are rejected. There
is no migration. Capacity must match the creation value on Open.

## Options

| Option | Default | Meaning |
| --- | ---: | --- |
| MaxLeaves | 2^22 | 16 GiB virtual capacity, range 2^8 through 2^32 leaves |
| Workers | 16 | Hash and snapshot workers, range 1 through 256 |
| RetainVersions | 32 | Completed block undo transitions retained in RAM |
| EpochBytes | 1 GiB | Journal file cut size; an oversized block stays intact |

Zero selects the default. State capacity changes roots. The other options do not.
Retention scales with recorded old bytes. Three snapshot buffers use additional
RAM; the image file uses page cache, which the kernel can reclaim. Allocator growth can require a contiguous free extent
and can copy the old region. Views copy state; no lazy fault machinery is used.

## Disk format (little endian)

Each journal file is `log/NNNNNNNNNNNNNNNNNNNN.journal`. A complete block frame is:

```
magic "PGJNL004" [8] | frame bytes u64 | version u64 | parent version u64 |
root [32] | flags u64 | redo payload | CRC-32C u32
```

Flags value 1 denotes Rewind; any other nonzero value is invalid. The redo
payload repeats `leaf id u64 | chunk mask u64 [2] | chunks`, in increasing leaf
order. Bit c of the mask (word c/64, bit c%64) marks the 32-byte chunk at leaf
offset 32c; the chunks follow in increasing c with their final bytes. A record
with an empty mask or a leaf outside MaxLeaves is invalid. Frame size includes
the header and checksum. CRC covers header and payload. Frames never cross epoch
files. The parent/version fields and rewind flag identify branch changes. After a
crash the first new frame's parent is the recovered version.

`image` holds the state bytes at their state offsets. CURRENT is 80 bytes:
`"PGIMG004" [8] | MaxLeaves u64 | journal epoch u64 | journal offset u64 |
version u64 | root [32] | reserved u32 | CRC-32C u32`. Its checksum covers the
preceding 76 bytes.

State leaf zero starts with `"PAGER003" [8] | MaxLeaves u64 | next region u64`.
At offset 4096 the region table holds `(offset u64, size u64)` entries indexed by
handle. Size zero means absent. Data starts at the next leaf after the table.

## Verification

```
go vet ./...
go test ./...
go test -race -count=1 ./...
PAGERDB_EVM_RETAIN=32 go test -run '^$' -bench '^BenchmarkEVM$' -benchtime=200x
PAGERDB_CRASH_ITERS=400 go test -run '^TestCrashRecovery$' -count=1 -v
PAGERDB_MEM_GIB=4 go test -run '^TestMemoryFootprint$' -count=1 -v
```

TestCrashRecovery repeatedly SIGKILLs a child process that runs a deterministic
workload (allocation changes, writes, partial undo, rewinds) with 64 KiB epochs
and a short applier interval. Each kill lands at a random time or with the child
frozen at a named step: batch start, mid-apply, after the image sync, after the
manifest temp write, after the rename, or right after a journal rotation. The
parent reopens the same directory each time and compares version, root and every
state byte with an in-process reference that repeats the child's steps. The
recovered head must not precede the last step the child flushed.

TestMemoryFootprint fills a state of the given size, waits for Checkpoint and
prints process memory (`/proc/self/status` on Linux, vmmap and footprint on
macOS). It uses only the public API, so it also measures older commits.

BenchmarkEVM keeps 2,000,000 slots of 64 bytes, 32-byte uint256 counters, 50,000
read/increment/write operations per block, uniform and normal sigma 1% / 0.1%.
It includes the final journal drain (Flush). After the timer it reports the image
lag (`max-lag-versions`, `end-lag-versions`, `max-lag-MB`), the Checkpoint
catch-up time, journal MB/s and, on Linux, peak RssAnon and device write MB/s
(`PAGERDB_DISK=nvme1n1`). Commit timings measure snapshot plus prior hash
wait, not blocking root calculation. Dirty-leaf counts use the 4 KiB constant.
Do not compare these timers directly with v2's blocking Commit/Seal timers.

`go test -run '^$' -bench '^BenchmarkCommitReusable$' -benchmem -benchtime=1000x`
measures a warmed write/Commit/Flush cycle at 8 and 512 dirty leaves. It includes
all journal work and excludes the image applier and epoch rotation.
`TestCommitAllocations` checks zero allocations across 32 and 256 blocks, worker
counts and retention settings, with GC between runs. It holds the image applier,
which allocates per batch (about once per second), not per block. Additional tests check
snapshot growth, unchanged capacity after smaller blocks, full-ring backpressure,
and rewind after repeated metadata reuse.
Evidence under `evidence/` predating v3 describes the old implementation.

## Phase 2

Sealing, sealed epochs/chapters, S3 storage, Export/Import, ViewRoot, history beyond
the RAM window remain deferred. Journal epochs before the image position are
deleted, so the journal no longer keeps history for a later sealer.
There is no Seal or pluggable EpochStore in this version. No PR or main-branch
merge is part of this rewrite.

## License

Ecosystem License v1.1. See LICENSE.md.
