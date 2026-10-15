package pagerdb

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

func openTest(t testing.TB, o Options) *DB {
	t.Helper()
	if o.MaxLeaves == 0 {
		o.MaxLeaves = 4096
	}
	db, err := Open(t.TempDir(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}
func allocTest(t testing.TB, db *DB, n int) Region {
	t.Helper()
	r, e := db.Alloc(n)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func commitTest(t testing.TB, db *DB) [32]byte {
	t.Helper()
	r, _, e := db.CommitWait()
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func must(t testing.TB, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}

func TestRoots(t *testing.T) {
	var expected [][32]byte
	for _, workers := range []int{1, 2, 16} {
		for run := 0; run < 2; run++ {
			db := openTest(t, Options{Workers: workers})
			r := allocTest(t, db, 2<<20)
			rng := rand.New(rand.NewSource(19))
			var roots [][32]byte
			for block := 0; block < 9; block++ {
				for range 700 {
					off := rng.Intn((2 << 20) - 80)
					data := make([]byte, 80)
					rng.Read(data)
					db.Write(r, off, data)
				}
				roots = append(roots, commitTest(t, db))
				must(t, db.Verify())
			}
			if expected == nil {
				expected = roots
			} else if !slices.Equal(roots, expected) {
				t.Fatal("roots differ across workers or runs")
			}
			must(t, db.Close())
		}
	}
}
func TestUndoAndAllocation(t *testing.T) {
	db := openTest(t, Options{})
	genesis := db.Root()
	m := db.Mark()
	r := allocTest(t, db, 5000)
	db.Write(r, 10, []byte("hello"))
	db.Undo(m)
	if _, e := db.Map(r); !errors.Is(e, ErrRegion) {
		t.Fatal(e)
	}
	if commitTest(t, db) != genesis {
		t.Fatal("allocation undo changed root")
	}
	r = allocTest(t, db, 5000)
	data, _ := db.Map(r)
	db.Write(r, 0, bytes.Repeat([]byte{3}, 5000))
	base := commitTest(t, db)
	m = db.Mark()
	db.Write(r, 15, bytes.Repeat([]byte{7}, 90))
	nested := db.Mark()
	db.Touch(r, 40, 90)
	clear(data[40:130])
	db.Undo(nested)
	if !bytes.Equal(data[15:105], bytes.Repeat([]byte{7}, 90)) {
		t.Fatal("nested undo")
	}
	db.Undo(m)
	if commitTest(t, db) != base {
		t.Fatal("overlap undo")
	}
	blocker := allocTest(t, db, 4096)
	db.Write(blocker, 0, []byte{9})
	commitTest(t, db)
	m = db.Mark()
	must(t, db.Grow(r, 20000))
	grown, _ := db.Map(r)
	if len(grown) != 20000 || !bytes.Equal(grown[:5000], bytes.Repeat([]byte{3}, 5000)) || !bytes.Equal(grown[5000:], make([]byte, 15000)) {
		t.Fatal("grow bytes")
	}
	db.Undo(m)
	old, _ := db.Map(r)
	if len(old) != 5000 || !bytes.Equal(old, bytes.Repeat([]byte{3}, 5000)) {
		t.Fatal("grow undo")
	}
	before := commitTest(t, db)
	m = db.Mark()
	must(t, db.Free(r))
	other := allocTest(t, db, 5000)
	fresh, _ := db.Map(other)
	if !bytes.Equal(fresh, make([]byte, 5000)) {
		t.Fatal("free leaked bytes")
	}
	db.Undo(m)
	if commitTest(t, db) != before {
		t.Fatal("free undo")
	}
	must(t, db.Verify())
}
func TestHistory(t *testing.T) {
	db := openTest(t, Options{RetainVersions: 3})
	r := allocTest(t, db, 9000)
	var roots [][32]byte
	for i := 0; i < 6; i++ {
		db.Write(r, 100, []byte{byte(i)})
		roots = append(roots, commitTest(t, db))
	}
	if _, e := db.View(2); !errors.Is(e, ErrWindow) {
		t.Fatal(e)
	}
	db.Write(r, 100, []byte{99})
	v, e := db.View(4)
	must(t, e)
	defer v.Close()
	b := make([]byte, 1)
	must(t, v.Read(r, 100, 1, b))
	if b[0] != 3 {
		t.Fatal(b)
	}
	if e = db.Rewind(4); !errors.Is(e, ErrPending) {
		t.Fatal(e)
	}
	db.Undo(Mark{db.generation, 0})
	must(t, db.Rewind(4))
	if db.Root() != roots[3] {
		t.Fatal("rewind root")
	}
	must(t, db.Verify())
	db.Write(r, 100, []byte{88})
	commitTest(t, db)
	must(t, v.Read(r, 100, 1, b))
	if b[0] != 3 {
		t.Fatal("view changed")
	}
	if _, e = db.RootAt(6); !errors.Is(e, ErrVersion) {
		t.Fatal(e)
	}
	dir := db.dir
	want := db.Root()
	must(t, db.Checkpoint())
	must(t, db.Close())
	re, e := Open(dir, Options{MaxLeaves: 4096})
	must(t, e)
	defer re.Close()
	if re.Version() != 5 || re.Root() != want {
		t.Fatal("rewind checkpoint recovery")
	}
}
func TestProof(t *testing.T) {
	db := openTest(t, Options{})
	r := allocTest(t, db, 3*LeafSize)
	db.Write(r, LeafSize-1, []byte("hello"))
	root := commitTest(t, db)
	for _, off := range []int{0, LeafSize - 1, LeafSize, 2 * LeafSize} {
		p, e := db.Proof(r, off)
		must(t, e)
		if !Verify(root, p) {
			t.Fatal("invalid proof")
		}
		p.Data[0] ^= 1
		if Verify(root, p) {
			t.Fatal("accepted changed leaf")
		}
	}
	db.Write(r, 0, []byte{1})
	if _, e := db.Proof(r, 0); !errors.Is(e, ErrPending) {
		t.Fatal(e)
	}
}
func TestCommitOverlap(t *testing.T) {
	db := openTest(t, Options{})
	serial := openTest(t, Options{Workers: 1})
	r := allocTest(t, db, 4<<20)
	s := allocTest(t, serial, 4<<20)
	commitTest(t, db)
	commitTest(t, serial)
	gate := make(chan struct{})
	entered := make(chan struct{})
	db.beforeHash = func() { close(entered); <-gate }
	db.Write(r, 0, bytes.Repeat([]byte{1}, 3<<20))
	serial.Write(s, 0, bytes.Repeat([]byte{1}, 3<<20))
	v, e := db.Commit()
	must(t, e)
	<-entered
	db.Write(r, 0, bytes.Repeat([]byte{2}, 3<<20))
	db.Touch(r, 3<<20, 4096)
	mapped, _ := db.Map(r)
	copy(mapped[3<<20:], bytes.Repeat([]byte{3}, 4096))
	close(gate)
	got, e := db.RootAt(v)
	must(t, e)
	if got != commitTest(t, serial) {
		t.Fatal("snapshot changed during hash")
	}
	db.beforeHash = nil
	serial.Write(s, 0, bytes.Repeat([]byte{2}, 3<<20))
	serial.Write(s, 3<<20, bytes.Repeat([]byte{3}, 4096))
	if commitTest(t, db) != commitTest(t, serial) {
		t.Fatal("next block differs")
	}
	must(t, db.Verify())
}
func TestJournalTornTail(t *testing.T) {
	db := openTest(t, Options{RetainVersions: 4})
	r := allocTest(t, db, 100)
	db.Write(r, 0, []byte("first"))
	commitTest(t, db)
	must(t, db.Checkpoint())
	db.Write(r, 0, []byte("second"))
	want := commitTest(t, db)
	must(t, db.Flush())
	dir := db.dir
	must(t, db.Close())
	paths, e := filepath.Glob(filepath.Join(dir, "log", "*.journal"))
	must(t, e)
	last := paths[len(paths)-1]
	// Model a crash in the middle of an append: the partial frame is dropped.
	f, e := os.OpenFile(last, os.O_APPEND|os.O_WRONLY, 0600)
	must(t, e)
	_, e = f.Write([]byte("torn frame"))
	must(t, e)
	must(t, f.Close())
	re, e := Open(dir, Options{MaxLeaves: 4096})
	must(t, e)
	if re.Version() != 2 || re.Root() != want {
		t.Fatal("tail recovery")
	}
	b := make([]byte, 6)
	re.Read(r, 0, 6, b)
	if string(b) != "second" {
		t.Fatal(string(b))
	}
	re.Write(r, 0, []byte("third!"))
	third := commitTest(t, re)
	must(t, re.Close())
	re, e = Open(dir, Options{MaxLeaves: 4096})
	must(t, e)
	if re.Version() != 3 || re.Root() != third {
		t.Fatal("append after torn tail")
	}
	must(t, re.Checkpoint())
	must(t, re.Close())
	// Corrupt published image bytes: the recovered root must not verify.
	f, e = os.OpenFile(filepath.Join(dir, "image"), os.O_RDWR, 0600)
	must(t, e)
	_, e = f.WriteAt([]byte{99}, int64(dataStart(4096))+1)
	must(t, e)
	must(t, f.Close())
	if db, e = Open(dir, Options{MaxLeaves: 4096}); e == nil {
		db.Close()
		t.Fatal("corruption accepted")
	}
}
func TestJournal(t *testing.T) {
	db := openTest(t, Options{EpochBytes: 512, RetainVersions: 2})
	r := allocTest(t, db, 4096)
	for i := 0; i < 10; i++ {
		db.Write(r, 0, bytes.Repeat([]byte{byte(i)}, 200))
		commitTest(t, db)
	}
	must(t, db.Flush())
	paths, e := filepath.Glob(filepath.Join(db.dir, "log", "*.journal"))
	must(t, e)
	if len(paths) < 2 {
		t.Fatal("no rotation")
	}
	var versions []uint64
	for _, path := range paths {
		data, e := os.ReadFile(path)
		must(t, e)
		for len(data) > 0 {
			if len(data) < frameHeader+4 || string(data[:8]) != "PGJNL004" {
				t.Fatal("header")
			}
			n := int(le.Uint64(data[8:]))
			if n > len(data) || n < frameHeader+4 {
				t.Fatal("length")
			}
			frame := data[:n]
			if checksum(frame[:n-4]) != le.Uint32(frame[n-4:]) {
				t.Fatal("checksum")
			}
			versions = append(versions, le.Uint64(frame[16:]))
			data = data[n:]
		}
	}
	if len(versions) != 10 {
		t.Fatal(versions)
	}
	for i, v := range versions {
		if v != uint64(i+1) {
			t.Fatal(versions)
		}
	}
}
func checksum(b []byte) uint32 { return crc32.Checksum(b, crcTable) }
func TestErrors(t *testing.T) {
	db := openTest(t, Options{MaxLeaves: 256})
	if _, e := db.Alloc(2 << 20); !errors.Is(e, ErrFull) {
		t.Fatal(e)
	}
	if _, e := Open(db.dir, Options{MaxLeaves: 256}); e == nil {
		t.Fatal("double open")
	}
	if _, e := db.RootAt(1); !errors.Is(e, ErrVersion) {
		t.Fatal(e)
	}
	must(t, db.Close())
	must(t, db.Close())
}

// Compare an incremental tree against a separate full binary reduction.
func TestTreeOracle(t *testing.T) {
	db := openTest(t, Options{MaxLeaves: 256})
	r := allocTest(t, db, 40000)
	db.Write(r, 7, []byte("tree oracle"))
	root := commitTest(t, db)
	level := make([][32]byte, 256)
	for i := range level {
		level[i] = sha256.Sum256(db.state[i*LeafSize : (i+1)*LeafSize])
	}
	for len(level) > 1 {
		for i := 0; i < len(level)/2; i++ {
			var pair [64]byte
			copy(pair[:32], level[2*i][:])
			copy(pair[32:], level[2*i+1][:])
			level[i] = sha256.Sum256(pair[:])
		}
		level = level[:len(level)/2]
	}
	if level[0] != root {
		t.Fatal("tree oracle mismatch")
	}
}

func TestAllocationHistoryModel(t *testing.T) {
	db := openTest(t, Options{RetainVersions: 50})
	rng := rand.New(rand.NewSource(41))
	type saved struct {
		root    [32]byte
		regions map[Region][]byte
	}
	var versions []saved
	live := map[Region][]byte{}
	for round := 0; round < 20; round++ {
		for range 6 {
			var ids []Region
			for r := range live {
				ids = append(ids, r)
			}
			slices.Sort(ids)
			switch rng.Intn(4) {
			case 0:
				r, e := db.Alloc(100 + rng.Intn(14000))
				must(t, e)
				m, _ := db.Map(r)
				live[r] = bytes.Clone(m)
			case 1:
				if len(ids) == 0 {
					continue
				}
				r := ids[rng.Intn(len(ids))]
				must(t, db.Free(r))
				delete(live, r)
			case 2:
				if len(ids) == 0 {
					continue
				}
				r := ids[rng.Intn(len(ids))]
				n := len(live[r]) + rng.Intn(17000)
				must(t, db.Grow(r, n))
				live[r] = append(live[r], make([]byte, n-len(live[r]))...)
			case 3:
				if len(ids) == 0 {
					continue
				}
				r := ids[rng.Intn(len(ids))]
				off := rng.Intn(len(live[r]))
				n := min(100, len(live[r])-off)
				data := make([]byte, n)
				rng.Read(data)
				db.Write(r, off, data)
				copy(live[r][off:], data)
			}
		}
		root := commitTest(t, db)
		must(t, db.Verify())
		copyLive := map[Region][]byte{}
		for r, data := range live {
			copyLive[r] = bytes.Clone(data)
		}
		versions = append(versions, saved{root, copyLive})
	}
	for i, want := range versions {
		v, e := db.View(uint64(i + 1))
		must(t, e)
		if v.Root() != want.root {
			t.Fatal("view root")
		}
		for r, data := range want.regions {
			got, e := v.Map(r)
			must(t, e)
			if !bytes.Equal(got, data) {
				t.Fatalf("version %d region %d", i+1, r)
			}
		}
		must(t, v.Close())
	}
	must(t, db.Rewind(7))
	if db.Root() != versions[6].root {
		t.Fatal("rewind")
	}
	must(t, db.Verify())
	dir := db.dir
	must(t, db.Close())
	re, e := Open(dir, Options{MaxLeaves: 4096})
	must(t, e)
	defer re.Close()
	if re.Root() != versions[6].root || re.Version() != 7 {
		t.Fatal("reopen")
	}
}

func TestJournalDoesNotBlockCommit(t *testing.T) {
	db := openTest(t, Options{RetainVersions: 2})
	r := allocTest(t, db, 4096)
	commitTest(t, db)
	must(t, db.Flush())
	gate, entered := make(chan struct{}), make(chan struct{})
	var once sync.Once
	db.beforeDisk = func() { once.Do(func() { close(entered); <-gate }) }
	db.Write(r, 0, []byte{1})
	_, e := db.Commit()
	must(t, e)
	<-entered
	// Commit and root calculation must finish even while the disk worker stops.
	finished := make(chan error, 1)
	go func() { db.Write(r, 0, []byte{2}); _, _, err := db.CommitWait(); finished <- err }()
	select {
	case err := <-finished:
		close(gate)
		must(t, err)
	case <-time.After(5 * time.Second):
		close(gate)
		<-finished
		t.Fatal("Commit waited on disk")
	}
	must(t, db.Flush())
	db.beforeDisk = nil
	must(t, db.Verify())
}
func TestDiskError(t *testing.T) {
	db, e := Open(t.TempDir(), Options{MaxLeaves: 256})
	must(t, e)
	r := allocTest(t, db, 100)
	commitTest(t, db)
	must(t, db.Flush())
	must(t, db.disk.journal.Close())
	db.Write(r, 0, []byte{1})
	_, e = db.Commit()
	must(t, e)
	if e = db.Flush(); !errors.Is(e, os.ErrClosed) {
		t.Fatal(e)
	}
	if _, e = db.Commit(); !errors.Is(e, os.ErrClosed) {
		t.Fatal(e)
	}
	if e = db.Close(); !errors.Is(e, os.ErrClosed) {
		t.Fatal(e)
	}
}

func TestMissingManifest(t *testing.T) {
	db := openTest(t, Options{})
	dir := db.dir
	must(t, db.Close())
	must(t, os.Remove(filepath.Join(dir, "CURRENT")))
	if reopened, e := Open(dir, Options{MaxLeaves: 4096}); e == nil {
		reopened.Close()
		t.Fatal("silently reset an existing store")
	}
}
func TestManifestRoot(t *testing.T) {
	db := openTest(t, Options{})
	dir := db.dir
	must(t, db.Close())
	path := filepath.Join(dir, "CURRENT")
	data, e := os.ReadFile(path)
	must(t, e)
	data[40] ^= 1
	le.PutUint32(data[76:], checksum(data[:76]))
	must(t, os.WriteFile(path, data, 0600))
	if reopened, e := Open(dir, Options{MaxLeaves: 4096}); e == nil {
		reopened.Close()
		t.Fatal("accepted a mismatched root")
	}
}

// A stalled applier never blocks Commit until the unapplied journal exceeds the
// lag bound; past it the disk worker waits, the ring fills and Commit waits.
func TestApplierLagBound(t *testing.T) {
	db := openTest(t, Options{RetainVersions: 2})
	r := allocTest(t, db, 4*LeafSize)
	commitTest(t, db)
	must(t, db.Checkpoint())
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()
	hold := func(point string) {
		if point == "apply" {
			<-gate
		}
	}
	db.disk.mu.Lock()
	db.disk.lagLimit = 1 << 20
	db.disk.mu.Unlock()
	db.diskHook.Store(&hold)
	db.Write(r, 0, []byte{1})
	commitTest(t, db) // Wakes the applier, which then holds in the hook.
	must(t, db.Flush())
	for i := 0; i < 50; i++ {
		db.Write(r, 0, bytes.Repeat([]byte{byte(i)}, 1000))
		commitTest(t, db)
	}
	must(t, db.Flush())
	if _, lag := db.disk.applyState(); lag == 0 {
		t.Fatal("applier was not held")
	}
	finished := make(chan error, 1)
	go func() {
		var err error
		for i := 0; i < 400 && err == nil; i++ {
			db.Write(r, 0, bytes.Repeat([]byte{byte(i)}, 4*LeafSize))
			_, err = db.Commit()
		}
		finished <- err
	}()
	select {
	case err := <-finished:
		t.Fatalf("Commit ignored the lag bound: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if _, lag := db.disk.applyState(); lag > 1<<20+64<<10 {
		t.Fatalf("lag %d exceeds the bound by more than one frame", lag)
	}
	db.diskHook.Store(nil)
	release()
	must(t, <-finished)
	must(t, db.Checkpoint())
	if v, lag := db.disk.applyState(); lag != 0 || v != db.Version() {
		t.Fatal("checkpoint left a lag")
	}
	must(t, db.Verify())
}
