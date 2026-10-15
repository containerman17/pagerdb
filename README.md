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
  discards the replaced suffix and checkpoints the result before returning.
  New commits reuse version numbers on the new branch. Existing Views survive.
- `Proof(r, off)` proves the leaf containing that byte at the committed head;
  pending writes are rejected. `Verify(root, proof)` checks the hash path. This
  proves the leaf, not the region-to-leaf mapping. `DB.Verify()` independently
  rebuilds the root from allocated state and checks the head.
- `Flush()` drains queued journal/checkpoint work but does not force a sync.
  `Checkpoint()` synchronously publishes the committed head. `Close()` drains
  work and discards pending writes; it does **not** create a new checkpoint.

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
precomputed empty hashes. No state file is mapped into the writer's address space.

Write and Touch append absolute offsets and old bytes to an in-RAM undo buffer,
then set dirty-leaf bits. They do not deduplicate: the caller can use first-write
tracking where its transaction semantics permit it. Mark/Undo need every change
since a mark. The packed buffer has no Go object per write.

Commit waits for the previous hash and a free snapshot slot, sorts dirty leaf IDs,
and copies leaves with the persistent worker pool. It submits an immutable snapshot.
Those same
workers hash leaves and fold sorted parent paths while the owner executes the
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

A separate disk worker appends one packed write per block with no per-block
fsync. It also applies snapshots to a private checkpoint image. Every K blocks,
it appends only leaves changed since the last checkpoint, including allocation
metadata, and fdatasyncs the checkpoint file (fsync on macOS). A synced temporary
manifest, atomic rename, and directory fsync publish the complete checkpoint.
Tree hashes are reconstructed and verified on Open rather than stored twice.
Checkpoint frames form an incremental image; compaction is deferred.

The disk queue is bounded by the three snapshot slots. If all are still busy,
Commit waits for the oldest consumer before reusing its storage. Thus a stalled
disk or checkpoint can apply backpressure after the ring fills. This replaces
v3's initial unbounded queue and GC-dependent snapshot pool. Disk errors become
sticky and surface at the next Commit, Flush, Checkpoint, or
Close. A commit already returned before a disk error may have a valid RAM root
but no journal record. This phase makes no durability promise for Commit.

## Recovery

Open takes an exclusive process lock, loads frames through CURRENT's published
end offset, checks CRC-32C, checks allocator bounds, and recomputes the root.
Unpublished checkpoint tails are truncated. It returns the checkpoint version;
the caller re-executes later blocks. Process crashes and OS crashes use the same
path. Journals are retained for future sealing, not replayed during phase 1.
Reopening starts a fresh journal epoch. A fresh store durably checkpoints version
zero. Corrupt published data is an error; it is not silently replaced.

Version 2 files are rejected. There is no v2 migration in phase 1. Capacity must
match the creation value on Open. The recovery protocol uses file and directory
sync ordering; physical power-cut testing remains unverified.

## Options

| Option | Default | Meaning |
| --- | ---: | --- |
| MaxLeaves | 2^22 | 16 GiB virtual capacity, range 2^8 through 2^32 leaves |
| Workers | 16 | Hash and snapshot workers, range 1 through 256 |
| RetainVersions | 32 | Completed block undo transitions retained in RAM |
| CheckpointInterval | 1000 | Commit versions between automatic checkpoints |
| EpochBytes | 1 GiB | Journal file cut size; an oversized block stays intact |

Zero selects the default. State capacity changes roots. The other options do not.
Retention scales with recorded old bytes. Three snapshot buffers and the checkpoint
image use additional RAM. Allocator growth can require a contiguous free extent
and can copy the old region. Views copy state; no lazy fault machinery is used.

## Disk format (little endian)

Each journal file is `log/NNNNNNNNNNNNNNNNNNNN.journal`. A complete block frame is:

```
magic "PGJNL003" [8] | frame bytes u64 | version u64 | parent version u64 |
root [32] | undo bytes u64 | flags u64 | reserved u64 |
undo payload | CRC-32C u32
```

Flags bit zero denotes Rewind. Other bits and reserved bytes are zero. The undo
payload repeats `absolute offset u64 | old bytes [length] | length u64`. Traverse
it backward from the final length. Frame size includes the header and checksum.
CRC covers header plus payload. Frames never cross epoch files. There is no
per-record checksum, compression, or new-byte image. The parent/version fields
and rewind flag identify branch changes for a later sealer. After a crash the
first frame's parent is the recovered checkpoint version. An incomplete final
frame must be ignored by a future journal reader.

The checkpoint file appends frames:

```
magic "PGCKP003" [8] | MaxLeaves u64 | version u64 | leaf count u64 |
root [32] | (leaf index u32 | leaf bytes [4096])* | CRC-32C u32
```

Leaf indexes are strictly increasing within a frame. CURRENT is 64 bytes:
`"PGCUR003" [8] | checkpoint end u64 | version u64 | root [32] |
reserved u32 | CRC-32C u32`. Its checksum covers the preceding 60 bytes.

State leaf zero starts with `"PAGER003" [8] | MaxLeaves u64 | next region u64`.
At offset 4096 the region table holds `(offset u64, size u64)` entries indexed by
handle. Size zero means absent. Data starts at the next leaf after the table.

## Verification

```
go vet ./...
go test ./...
go test -race -count=1 ./...
PAGERDB_EVM_RETAIN=32 go test -run '^$' -bench '^BenchmarkEVM$' -benchtime=200x
```

BenchmarkEVM keeps 2,000,000 slots of 64 bytes, 32-byte uint256 counters, 50,000
read/increment/write operations per block, uniform and normal sigma 1% / 0.1%.
It includes the final disk drain. Commit timings measure snapshot plus prior hash
wait, not blocking root calculation. Dirty-leaf counts use the 4 KiB constant.
Do not compare these timers directly with v2's blocking Commit/Seal timers.

`go test -run '^$' -bench '^BenchmarkCommitReusable$' -benchmem -benchtime=1000x`
measures a warmed write/Commit/Flush cycle at 8 and 512 dirty leaves. It includes
all background work and excludes automatic checkpoints and epoch rotation.
`TestCommitAllocations` checks zero allocations across 32 and 256 blocks, worker
counts and retention settings, with GC between runs. Additional tests check
snapshot growth, unchanged capacity after smaller blocks, full-ring backpressure,
and rewind after repeated metadata reuse.
Evidence under `evidence/` predating v3 describes the old implementation.

## Phase 2

Sealing, sealed epochs/chapters, S3 storage, Export/Import, ViewRoot, history beyond
the RAM window, checkpoint compaction and journal reclamation remain deferred.
There is no Seal or pluggable EpochStore in this version. No PR or main-branch
merge is part of this rewrite.

## License

Ecosystem License v1.1. See LICENSE.md.
