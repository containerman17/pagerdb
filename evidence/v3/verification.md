# Phase 1 verification

2026-10-15 JST. macOS arm64 and a local Ubuntu arm64 VM, Go 1.26.0.

Passed on both platforms: `go vet ./...`, `go test ./...`, and
`go test -race -count=1 ./...`. Raw test summaries are adjacent to this file.
Linux uses the same sources copied to a local VM, not a cloud machine.

Tests cover an independent complete-tree oracle, deterministic roots across
worker counts, nested and overlapping undo, allocation rollback, randomized
allocation/history comparison, detached views, rewind and branch recovery,
proof tampering, a deliberately blocked hash with concurrent next-block writes,
a deliberately blocked disk with successful commits, sticky disk errors,
journal framing/rotation, corrupt and unpublished checkpoint tails, and a child
process killed after checkpoint version 2 with committed version 3 and pending
writes. Open returns version 2 and its verified bytes/root.

Performance is unmeasured. BenchmarkEVM is ported with 4 KiB leaf accounting and
asynchronous Commit. The old sealer and its tests are removed with phase 2 code.
Physical power cuts and hosted CI execution are not verified by these local runs.
