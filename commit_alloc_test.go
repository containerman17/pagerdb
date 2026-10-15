package pagerdb

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// Warm every metadata, undo, dirty-ID and snapshot slot to its workload size.
// The measured operation includes the background hash and journal, so their
// allocations cannot be hidden by returning from Commit early. The image
// applier is held: it runs about once per second, not once per block.
func allocationWorkload(t testing.TB, leaves, workers int, retain uint64) (*DB, func()) {
	t.Helper()
	db := openTest(t, Options{Workers: workers, RetainVersions: retain, EpochBytes: 1 << 40})
	// The applier allocates per batch (paths, manifest). Hold it so only the
	// per-block path is counted; the release runs before db.Close.
	gate := make(chan struct{})
	hold := func(point string) {
		if point == "apply" {
			<-gate
		}
	}
	db.diskHook.Store(&hold)
	t.Cleanup(func() { close(gate) })
	r := allocTest(t, db, leaves*LeafSize)
	var value [1]byte
	step := func() {
		value[0]++
		for i := 0; i < leaves; i++ {
			db.Write(r, i*LeafSize, value[:])
		}
		if _, err := db.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	for range 2*len(db.blocks) + 2*snapshotCount {
		step()
	}
	return db, step
}
func TestCommitAllocations(t *testing.T) {
	for _, tc := range []struct {
		leaves, workers int
		retain          uint64
	}{{8, 1, 1}, {512, 16, 2}, {512, 16, 32}} {
		t.Run(fmt.Sprintf("leaves%d_workers%d_retain%d", tc.leaves, tc.workers, tc.retain), func(t *testing.T) {
			db, step := allocationWorkload(t, tc.leaves, tc.workers, tc.retain)
			var addresses [snapshotCount]*byte
			for i := range db.snapshots {
				addresses[i] = &db.snapshots[i].data[0]
			}
			short := testing.AllocsPerRun(32, step)
			runtime.GC()
			long := testing.AllocsPerRun(256, step)
			if short != 0 || long != 0 {
				t.Fatalf("steady-state allocations/block: short=%g long=%g, want zero", short, long)
			}
			for i := range db.snapshots {
				if &db.snapshots[i].data[0] != addresses[i] {
					t.Fatal("GC or steady commits replaced snapshot storage")
				}
			}
			must(t, db.Verify())
		})
	}
}
func BenchmarkCommitReusable(b *testing.B) {
	for _, leaves := range []int{8, 512} {
		b.Run(fmt.Sprintf("leaves%d", leaves), func(b *testing.B) {
			_, step := allocationWorkload(b, leaves, 16, 32)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				step()
			}
		})
	}
}

func TestSnapshotRingBackpressure(t *testing.T) {
	db := openTest(t, Options{RetainVersions: 2})
	r := allocTest(t, db, LeafSize)
	commitTest(t, db)
	must(t, db.Flush())
	gate, entered := make(chan struct{}), make(chan struct{})
	first := true
	db.beforeDisk = func() {
		if first {
			first = false
			close(entered)
			<-gate
		}
	}
	for i := 1; i <= snapshotCount; i++ {
		db.Write(r, 0, []byte{byte(i)})
		commitTest(t, db)
		if i == 1 {
			<-entered
		}
	}
	// All three immutable images must coexist while their disk consumer is blocked.
	seen := [snapshotCount + 1]bool{}
	for i := range db.snapshots {
		slot := &db.snapshots[i]
		v := slot.data[0]
		if v < 1 || v > snapshotCount || seen[v] {
			close(gate)
			t.Fatal("snapshot was overwritten before consumption")
		}
		seen[v] = true
	}
	enteredCommit, finished := make(chan struct{}), make(chan error, 1)
	go func() { db.Write(r, 0, []byte{4}); close(enteredCommit); _, err := db.Commit(); finished <- err }()
	<-enteredCommit
	select {
	case err := <-finished:
		close(gate)
		t.Fatalf("fourth Commit bypassed a full ring: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(gate)
	must(t, <-finished)
	must(t, db.Flush())
	db.beforeDisk = nil
	must(t, db.Checkpoint())
	must(t, db.Verify())
	dir, root := db.dir, db.Root()
	must(t, db.Close())
	re, err := Open(dir, Options{MaxLeaves: 4096})
	must(t, err)
	defer re.Close()
	if re.Root() != root {
		t.Fatal("checkpoint image differs after ring reuse")
	}
}

func TestRewindAfterRingReuse(t *testing.T) {
	db := openTest(t, Options{RetainVersions: 3})
	r := allocTest(t, db, LeafSize)
	roots := map[uint64][32]byte{}
	for i := 0; i < 80; i++ {
		db.Write(r, 0, []byte{byte(i)})
		root := commitTest(t, db)
		roots[db.Version()] = root
		if i%7 == 6 {
			target := db.Version() - 1
			must(t, db.Rewind(target))
			if db.Root() != roots[target] {
				t.Fatal("rewind after metadata reuse")
			}
			must(t, db.Checkpoint())
			must(t, db.Verify())
		}
	}
}

func TestSnapshotHighWater(t *testing.T) {
	db := openTest(t, Options{RetainVersions: 2})
	r := allocTest(t, db, 1024*LeafSize)
	commitTest(t, db)
	must(t, db.Flush())
	fill := func(n int, value byte) {
		for i := 0; i < n; i++ {
			db.Write(r, i*LeafSize, []byte{value})
		}
		commitTest(t, db)
		must(t, db.Flush())
	}
	for range snapshotCount {
		fill(512, 0xa5)
	}
	var addresses [snapshotCount]*byte
	for i := range db.snapshots {
		addresses[i] = &db.snapshots[i].data[0]
	}
	runtime.GC()
	for range 2 * snapshotCount {
		fill(16, 0x5a)
	}
	for i := range db.snapshots {
		slot := &db.snapshots[i]
		if &slot.data[0] != addresses[i] {
			t.Fatal("smaller block replaced storage")
		}
		if slot.data[:cap(slot.data)][511*LeafSize] != 0xa5 {
			t.Fatal("unused snapshot capacity was cleared")
		}
	}
	for range snapshotCount {
		fill(1024, 0x39)
	}
	for i := range db.snapshots {
		slot := &db.snapshots[i]
		if len(slot.data) != 1024*LeafSize || &slot.data[0] == addresses[i] {
			t.Fatal("larger block did not grow its slot")
		}
	}
	must(t, db.Verify())
}
