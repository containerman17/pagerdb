# Final verification

Recorded 2026-10-14 13:06 JST. Source commit: 676c599.

Go 1.26.0 on all targets. macOS pages: 16384 bytes. Linux pages: 4096 bytes.
All final vet commands exited 0. All final normal and race suites passed.
Every metric in xor-benchmark-ranges.json contains three values from a passing run.

```
macos test-final: ok  	github.com/containerman17/pagerdb	20.260s
macos race-final: ok  	github.com/containerman17/pagerdb	59.384s
linux-arm64 test-final: ok  	github.com/containerman17/pagerdb	15.724s
linux-arm64 race-final: ok  	github.com/containerman17/pagerdb	65.675s
linux-amd64 test-final: ok  	github.com/containerman17/pagerdb	41.897s
linux-amd64 race-final: ok  	github.com/containerman17/pagerdb	175.386s
```

The six two-minute fuzz campaigns passed. Parser seed corpora are committed.
Linux native userfaultfd and the !cgo fallback passed.
The exported-comment audit, gofmt check, forbidden-text scan and git diff --check passed.

Cleanup: deleted pagerdb-arm64 and pagerdb-amd64. The existing ubuntu machine remains.
orb stop exited 0; orb status returned Stopped. Temporary SDKs, caches and test artifacts
under .scratch are removed before delivery. No push or remote repository change occurred.
