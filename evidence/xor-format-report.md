# Backward XOR format implementation

This report supersedes the forward-frame implementation and measurements in epoch-report.md. The format redirect keeps the rest of the product work.

## Format

PGEPOCH3, all fixed-width integers little endian.

Header (64 bytes): magic[8], epoch u64, checkpoint/start version u64, end version u64, cumulative leaf count u64, index offset u64, root count u64, root offset u64. Relative record versions subtract the checkpoint/start version. Epoch span must fit uint32; sealing returns an actionable error to reduce EpochVersions otherwise.

Physical order: frame payloads, frame directories, dense lists, cumulative leaf index, roots. No base leaf starts a frame.

Frame header (32 bytes): flags u8, zero padding[3], stored payload length u32, decoded payload length u32, record count u32, first record ordinal u64, oldest relative version u32, newest relative version u32. Flag bit0 means zstd; bit1 means a closing full copy. No other flag bits are accepted.

Record: relative version u32, leaf offset u16, trimmed length u16, XOR bytes[length]. Records are newest first, including reverse log order for multiple writes in one version. Leading and trailing zero XOR bytes are removed. Zero-length changes produce no record.

Runs contain at most262144record bytes. A run that cannot fit the next record closes with a1024-byte leaf copy after all its records have been undone. A full last run also gets a copy; a partial last run does not. A rarely changed leaf has no full copy. Copies can represent a point between two writes of the same version, so a copy is usable only if every record before it is newer than the target version. The reader then continues undoing remaining records. Maximum decoded frame size263168bytes. zstd fastest, encoder concurrency1; compression is kept only at a saving of at least10%.

Per-leaf frame directory:24bytes per frame, frame offset u64, first record ordinal u64, record count u32, flags u32.

Per-leaf dense list:8bytes per record, relative version u32, offset u16, length u16. Same newest-first order as frame records. These uncompressed rows are additional to frame payload storage.

Cumulative leaf index:48bytes per leaf, leaf u32, frame count u32, directory offset u64, latest changed epoch u64, previous changed epoch u64, dense offset u64, record count u64. Inherited rows keep only leaf and latest changed epoch; all other fields are zero. Rows are sorted by leaf. This duplicates sparse cumulative rows across epoch files, a disk-space tradeoff for bounded memory and direct lookup.

Roots:40bytes each, hash[32] and version u64, sorted by hash with equal roots in chronological order. Every committed version and the checkpoint have a root row, including versions with no data changes. Root lookup returns the newest match.

## Sealing and bounds

`epoch.go` and `epoch_sort.go`: sort40-byte log undo references on disk with4096-row sort batches and reused merge buffers. One working leaf, XOR buffer, raw frame buffer and compressed frame buffer serve each leaf in turn. Dense rows and directories stream through temporary files. No epoch-sized temporary arrays remain.

Touch new bytes are captured at Commit and can overlap later Write or direct mapped mutations. Literal old XOR stored-new is therefore not correct for every Touch record. The sealer reconstructs the epoch-end working leaf once, walks log records newest first, emits old XOR current-working bytes, and updates the working bytes to old. This produces canonical backward changes, including overlapping Touch and Write operations. Trimming happens only after this normalization. The working leaf is never stored except at full-run boundaries.

This epoch-end reconstruction walks later live log entries for the same leaf. Long sealing backlogs can therefore increase sealing CPU cost. Correctness and fixed temporary memory take precedence; final sustained results belong to the coordinator's benchmark report.

## Reads and pruning

`view.go` sends each requested leaf range to `sealedRange`. Whole-leaf users, including Export and View.Map, retain the `sealedLeaf` wrapper.

The reader rolls the live state back to the latest sealed boundary once. It finds the leaf's latest changed epoch from the cumulative index, then follows previous-change epoch pointers backward. Retained logs overlapping sealed history are not applied twice. A pre-opened view at a rewind target also works when the latest remaining sealed epoch ends before that target, by falling back to the live window.

Within each epoch, binary search the dense version list, scan newer entries for range overlap, and return the current range unchanged if none overlaps. Otherwise use the nearest qualifying full-run copy and apply overlapping XOR bytes. Index searches are O(log indexed leaves) per changed epoch; the history walk is proportional to changed epochs visited, not a blanket O(log total history) claim. Dense overlap scans are proportional to candidate newer records. Full run copies reduce payload replay within an epoch.

A global cache holds at most64file pages (256KiB). Indexes and dense lists stay on disk. Only bounded frame payloads are decoded. Epoch descriptors remain proportional to epoch count.

`loadSealed` accepts a missing contiguous prefix using os.ErrNotExist and rejects interior gaps. EpochStore.Open now documents that missing-file error. Oldest files can be deleted offline without rebasing remaining files. No new public pruning API was added. Views older than the surviving sealed prefix are unavailable unless retained live history still serves them.

## Tests and evidence

`TestXORTrimNoopAndSameVersionOrder`: exact trimming, omitted no-op writes, newest-first same-version records, no copy for rare leaves, and dense-list disjoint-range fast path without payload reads.

`TestXORRunCopies` and `TestXORSameVersionRunCopies`: multiple256KiB runs, full-copy boundaries, reads around boundaries and copies between writes of the same version.

`TestXOROverlappingTouchesAndWrites`: Touch, overlapping Write, further mapped mutations, repeated Touch, cross-leaf touches, sealing and reopen.

`TestXORFrameValidation`, `TestXORDenseIndexValidation`, `TestXOREpochVersionBounds`: malformed flags/sizes/counts, dense bounds/order metadata and actionable version-span rejection. Existing `TestEpochCumulativeLookup`, `TestSealedRootReturnsNewest`, `TestDiskSort` and `TestViews` remain applicable. The separate xor_history_test.go covers retained/pruned histories, rewind/rewrite, pending writes, old View.Map and Export, missing oldest files and rejected interior gaps.

`go test -race -run 'Test(XOR|EpochCumulative|SealedRoot)|TestViews' -count=1`: PASS9.054s. Exact output xor-focused-race.txt.

`GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzEpochFrame$' -fuzztime=2m`: PASS120.179s,7,268,360executions,total corpus297.

`GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzEpochIndex$' -fuzztime=2m`: PASS120.769s,10,305,021executions,total corpus19. This fuzzer validates dense indexes and exercises frame replay for parsed leaf rows. Counts include the native fuzz cache.

Committed seeds include a raw record frame, a compressed full-copy run, a valid dense-index image and truncated inputs. `go test -run '^FuzzEpoch' -count=1`: PASS0.091s. Parser code did not change after the two-minute fuzz runs; only sealing normalization and regression tests changed.

`go test -run '^TestSealMemoryMeasure$' -v -count=1`: broad1MiB old/new payload across8192leaves sampled peak extra heap3,606,016B,total allocation4,559,072B,97ms; broad8MiB across65536leaves peak10,267,032B,total17,383,448B,622ms. Repeated single-leaf cases sampled peaks11,296,624B and14,213,352B. Samples exclude existing mmap pages and OS page cache, and include garbage waiting for collection. Exact output xor-memory.txt.

The requested workload bytes-per-record and age1/100/10000 latency measurements are owned by xor_bench_test.go and the coordinator. No broad benchmarks were started by this implementation task. Full macOS/Linux suites and native userfaultfd validation are owned by the coordinator; this report makes no unverified platform claim.
