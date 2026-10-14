package pagerdb

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRetainedRewindAcrossEpochs(t *testing.T) {
	dir := t.TempDir()
	o := Options{MaxLeaves: 1 << 14, EpochVersions: 3, RetainVersions: 12}
	db, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Alloc(1024)
	if err != nil {
		t.Fatal(err)
	}
	roots := map[uint64][32]byte{}
	for i := byte(1); i <= 10; i++ {
		db.Write(r, 0, []byte{i})
		root, v, e := db.Commit()
		if e != nil {
			t.Fatal(e)
		}
		roots[v] = root
	}
	if err := db.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := db.Rewind(4); err != nil {
		t.Fatal(err)
	}
	if db.Root() != roots[4] {
		t.Fatal("rewind root")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, v := range []uint64{1, 3, 4} {
		vw, e := db.View(v)
		if e != nil {
			t.Fatal(e)
		}
		b := make([]byte, 1)
		if e = vw.Read(r, 0, 1, b); e != nil {
			t.Fatal(e)
		}
		vw.Close()
		if b[0] != byte(v) {
			t.Fatalf("version %d: %d", v, b[0])
		}
	}
	db.Write(r, 0, []byte{99})
	if _, v, e := db.Commit(); e != nil || v != 5 {
		t.Fatalf("new history %d: %v", v, e)
	}
	vw, e := db.View(4)
	if e != nil {
		t.Fatal(e)
	}
	defer vw.Close()
	b := make([]byte, 1)
	if e = vw.Read(r, 0, 1, b); e != nil || b[0] != 4 {
		t.Fatalf("old view: %v %v", b, e)
	}
}

// A state page changed without a surviving log record must never pass Open.
func TestRecoveryLostWholeLogTail(t *testing.T) {
	dir := t.TempDir()
	o := Options{MaxLeaves: 1 << 14}
	db, e := Open(dir, o)
	if e != nil {
		t.Fatal(e)
	}
	r, e := db.Alloc(1024)
	if e != nil {
		t.Fatal(e)
	}
	db.Write(r, 0, []byte{1})
	if _, _, e = db.Commit(); e != nil {
		t.Fatal(e)
	}
	if e = db.Seal(); e != nil {
		t.Fatal(e)
	}
	// A byte no kept log entry covers: recovery redoes byte 0 from the log.
	off := db.loc(r).off + 1
	end := db.cur.tail
	path := db.cur.f.Name()
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(filepath.Join(dir, "state"), os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.WriteAt([]byte{99}, int64(off)); e != nil {
		t.Fatal(e)
	}
	f.Close()
	if e = os.Truncate(path, end); e != nil {
		t.Fatal(e)
	}
	os.Remove(filepath.Join(dir, "clean"))
	db, e = Open(dir, o)
	if e == nil {
		db.Close()
		t.Fatal("accepted state bytes without log record")
	}
	if !strings.Contains(e.Error(), "root") {
		t.Fatal(e)
	}
}

func TestRecoveryPendingTouch(t *testing.T) {
	dir := t.TempDir()
	o := Options{MaxLeaves: 1 << 14}
	db, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Alloc(1024)
	if err != nil {
		t.Fatal(err)
	}
	db.Write(r, 0, []byte{7})
	root, v, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	m, err := db.Map(r)
	if err != nil {
		t.Fatal(err)
	}
	db.Touch(r, 0, 1)
	m[0] = 42
	// Force the uncommitted shared state to storage, as a power loss can do.
	if err = db.syncState(); err != nil {
		t.Fatal(err)
	}
	crash(db)
	db, err = Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.Root() != root || db.Version() != v {
		t.Fatal("lost committed root")
	}
	got := make([]byte, 1)
	db.Read(r, 0, 1, got)
	if got[0] != 7 {
		t.Fatal(got)
	}
}

func TestRewindRetentionBoundary(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14, EpochVersions: 2, RetainVersions: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(1024)
	if err != nil {
		t.Fatal(err)
	}
	for i := byte(1); i <= 10; i++ {
		db.Write(r, 0, []byte{i})
		if _, _, err = db.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Seal(); err != nil {
		t.Fatal(err)
	}
	if err = db.Rewind(6); err != ErrWindow {
		t.Fatalf("outside retention: %v", err)
	}
	if err = db.Rewind(7); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentTouchView(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(1024)
	if err != nil {
		t.Fatal(err)
	}
	db.Write(r, 0, []byte{7})
	if _, _, err = db.Commit(); err != nil {
		t.Fatal(err)
	}
	vw, err := db.View(1)
	if err != nil {
		t.Fatal(err)
	}
	defer vw.Close()
	m, err := db.Map(r)
	if err != nil {
		t.Fatal(err)
	}
	// Touch publishes old bytes while holding db.mu before the owner writes.
	db.Touch(r, 0, 1)
	done := make(chan error, 1)
	go func() {
		b := make([]byte, 1)
		for range 1000 {
			if e := vw.Read(r, 0, 1, b); e != nil {
				done <- e
				return
			}
			if b[0] != 7 {
				done <- fmt.Errorf("view byte %d", b[0])
				return
			}
		}
		done <- nil
	}()
	for i := range 1000 {
		m[0] = byte(i)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryInterruptedRewind(t *testing.T) {
	for _, stage := range []string{"intent", "partial state", "removed log"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			o := Options{MaxLeaves: 1 << 14, EpochVersions: 2}
			db, err := Open(dir, o)
			if err != nil {
				t.Fatal(err)
			}
			r, err := db.Alloc(1024)
			if err != nil {
				t.Fatal(err)
			}
			var target [32]byte
			for i := byte(1); i <= 8; i++ {
				db.Write(r, 0, []byte{i})
				root, _, e := db.Commit()
				if e != nil {
					t.Fatal(e)
				}
				if i == 3 {
					target = root
				}
			}
			if err = db.Seal(); err != nil {
				t.Fatal(err)
			}
			db.mu.Lock()
			var b block
			for _, candidate := range db.blocks {
				if candidate.ver == 3 {
					b = candidate
					break
				}
			}
			if err = db.writeRewindIntent(3, b.c.epoch, b.end); err != nil {
				t.Fatal(err)
			}
			if stage == "partial state" {
				m, off, n := db.rec(&db.entries[len(db.entries)-1])
				copy(db.state[off:off+n], m[entryHdr:entryHdr+n])
				if err = db.syncState(); err != nil {
					t.Fatal(err)
				}
			}
			if stage == "removed log" {
				for i := len(db.entries) - 1; i >= 0; i-- {
					if db.entries[i].ver <= 3 {
						break
					}
					m, off, n := db.rec(&db.entries[i])
					copy(db.state[off:off+n], m[entryHdr:entryHdr+n])
				}
				if err = db.syncState(); err != nil {
					t.Fatal(err)
				}
				if err = os.Remove(db.cur.f.Name()); err != nil {
					t.Fatal(err)
				}
				if err = syncDir(filepath.Join(dir, "log")); err != nil {
					t.Fatal(err)
				}
			}
			db.mu.Unlock()
			crash(db)
			db, err = Open(dir, o)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if db.Version() != 3 || db.Root() != target {
				t.Fatalf("recovered %d with root %x", db.Version(), db.Root())
			}
			db.Write(r, 0, []byte{99})
			if _, v, e := db.Commit(); e != nil || v != 4 {
				t.Fatalf("new history %d %v", v, e)
			}
		})
	}
}

func TestRecoveryIncompleteRotation(t *testing.T) {
	for _, n := range []int64{0, 16, cpRec} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			dir := t.TempDir()
			o := Options{MaxLeaves: 1 << 14}
			db, err := Open(dir, o)
			if err != nil {
				t.Fatal(err)
			}
			r, err := db.Alloc(1024)
			if err != nil {
				t.Fatal(err)
			}
			db.Write(r, 0, []byte{42})
			root, v, err := db.Commit()
			if err != nil {
				t.Fatal(err)
			}
			path := db.chunkPath(db.cur.epoch + 1)
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, make([]byte, n), 0o644); err != nil {
				t.Fatal(err)
			}
			db, err = Open(dir, o)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if db.Root() != root || db.Version() != v {
				t.Fatal("lost root during rotation")
			}
		})
	}
}

func TestRecoveryTornRecordChecksums(t *testing.T) {
	for _, kind := range []string{"entry", "block", "checkpoint"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			o := Options{MaxLeaves: 1 << 14}
			db, err := Open(dir, o)
			if err != nil {
				t.Fatal(err)
			}
			r, err := db.Alloc(1024)
			if err != nil {
				t.Fatal(err)
			}
			db.Write(r, 0, []byte{7})
			want, version, err := db.Commit()
			if err != nil {
				t.Fatal(err)
			}
			path := db.cur.f.Name()
			var at int64
			switch kind {
			case "entry":
				// Construct a torn next undo record before its corresponding state write.
				at = db.cur.tail
				if err = db.Close(); err != nil {
					t.Fatal(err)
				}
				e := make([]byte, entryHdr+2)
				e[0] = 'E'
				le.PutUint32(e[8:], 1)
				le.PutUint64(e[16:], version+1)
				f, e2 := os.OpenFile(path, os.O_RDWR, 0)
				if e2 != nil {
					t.Fatal(e2)
				}
				_, err = f.WriteAt(e, at)
				f.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "block":
				db.Write(r, 0, []byte{99})
				if _, _, err = db.Commit(); err != nil {
					t.Fatal(err)
				}
				at = db.cur.tail - blockRec + 16
				if err = db.Close(); err != nil {
					t.Fatal(err)
				}
			case "checkpoint":
				if err = db.Seal(); err != nil {
					t.Fatal(err)
				}
				path = db.cur.f.Name()
				at = 16
				if err = db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if kind != "entry" {
				f, e := os.OpenFile(path, os.O_RDWR, 0)
				if e != nil {
					t.Fatal(e)
				}
				var b [1]byte
				if _, e = f.ReadAt(b[:], at); e != nil {
					t.Fatal(e)
				}
				b[0] ^= 255
				if _, e = f.WriteAt(b[:], at); e != nil {
					t.Fatal(e)
				}
				f.Close()
			}
			db, err = Open(dir, o)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if db.Root() != want || db.Version() != version {
				t.Fatalf("got %d %x", db.Version(), db.Root())
			}
			db.Write(r, 0, []byte{55})
			if _, _, err = db.Commit(); err != nil {
				t.Fatal(err)
			}
			if err = db.Seal(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRecoveryRejectsStateBounds(t *testing.T) {
	for _, kind := range []string{"short file", "long file", "log offset"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			o := Options{MaxLeaves: 1 << 14}
			db, err := Open(dir, o)
			if err != nil {
				t.Fatal(err)
			}
			logPath, end := db.cur.f.Name(), db.cur.tail
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "short file":
				err = os.Truncate(filepath.Join(dir, "state"), 1)
			case "long file":
				err = os.Truncate(filepath.Join(dir, "state"), int64(o.MaxLeaves*LeafSize+SegSize))
			case "log offset":
				e := make([]byte, entryHdr+2)
				e[0] = 'E'
				le.PutUint32(e[8:], 1)
				le.PutUint64(e[16:], 1)
				le.PutUint64(e[24:], o.MaxLeaves*LeafSize)
				le.PutUint32(e[12:], entryChecksum(e, 1))
				f, e2 := os.OpenFile(logPath, os.O_RDWR, 0)
				if e2 != nil {
					t.Fatal(e2)
				}
				_, err = f.WriteAt(e, end)
				f.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			db, err = Open(dir, o)
			if err == nil {
				db.Close()
				t.Fatal("accepted corrupt state bounds")
			}
		})
	}
}

func TestRecoveryRebuildBatches(t *testing.T) {
	dir := t.TempDir()
	o := Options{MaxLeaves: 1 << 18}
	db, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Alloc(70 << 20)
	if err != nil {
		t.Fatal(err)
	}
	for off := 0; off < 70<<20; off += LeafSize {
		db.Write(r, off, []byte{byte(off/LeafSize%255 + 1)})
	}
	root, _, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.Root() != root {
		t.Fatal("batched rebuild root differs")
	}
	if cap(db.dirty) > 65536 {
		t.Fatalf("rebuild retained %d dirty slots", cap(db.dirty))
	}
	var got [1]byte
	for _, off := range []int{0, 65535 * LeafSize, 65536 * LeafSize, (70 << 20) - LeafSize} {
		db.Read(r, off, 1, got[:])
		if got[0] != byte(off/LeafSize%255+1) {
			t.Fatalf("offset %d: %d", off, got[0])
		}
	}
}

func TestRecoveryFailurePreservesUndo(t *testing.T) {
	dir := t.TempDir()
	o := Options{MaxLeaves: 1 << 14}
	db, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Alloc(2048)
	if err != nil {
		t.Fatal(err)
	}
	db.Write(r, 0, []byte{7})
	db.Write(r, 1024, []byte{8})
	if _, _, err = db.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = db.Seal(); err != nil {
		t.Fatal(err)
	}
	pos, path := db.cur.tail, db.cur.f.Name()
	db.Write(r, 0, []byte{9})
	off := db.loc(r).off + 1025 // redo restores byte 1024, not this one
	if err = db.syncState(); err != nil {
		t.Fatal(err)
	}
	crash(db)
	f, err := os.OpenFile(filepath.Join(dir, "state"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt([]byte{99}, int64(off)); err != nil {
		t.Fatal(err)
	}
	f.Close()
	db, err = Open(dir, o)
	if err == nil {
		db.Close()
		t.Fatal("accepted corrupt state")
	}
	f, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var tag [1]byte
	if _, err = f.ReadAt(tag[:], pos); err != nil {
		t.Fatal(err)
	}
	if tag[0] != 'E' {
		t.Fatal("failed recovery destroyed its undo record")
	}
}

// TestRecoveryRedoesKeptChunks covers a crash after several rotations whose
// state was never synced: the state file on disk is as old as the oldest kept
// checkpoint, and recovery must redo every kept chunk to reach the last block.
func TestRecoveryRedoesKeptChunks(t *testing.T) {
	dir := t.TempDir()
	o := Options{MaxLeaves: 1 << 14, EpochVersions: 3}
	db, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Alloc(8 << 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = db.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	if db, err = Open(dir, o); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(3, 4))
	want := make([]byte, 8<<10)
	db.Read(r, 0, len(want), want)
	var root [32]byte
	var ver uint64
	for range 20 {
		for range 5 {
			b := make([]byte, 1+rng.IntN(2000))
			off := rng.IntN(len(want) - len(b))
			for i := range b {
				b[i] = byte(rng.Uint32())
			}
			db.Write(r, off, b)
			copy(want[off:], b)
		}
		if root, ver, err = db.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if len(db.chunks) < 5 {
		t.Fatalf("only %d chunks kept", len(db.chunks))
	}
	crash(db)
	if err = os.WriteFile(filepath.Join(dir, "state"), old, 0o644); err != nil {
		t.Fatal(err)
	}
	if db, err = Open(dir, o); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.Version() != ver || db.Root() != root {
		t.Fatalf("recovered version %d, want %d", db.Version(), ver)
	}
	got := make([]byte, len(want))
	db.Read(r, 0, len(got), got)
	if !bytes.Equal(got, want) {
		t.Fatal("recovered bytes differ")
	}
}

// TestLogTrimWaitsForDurableState checks that the oldest kept chunk always
// starts at a checkpoint the synced state file covers. A view pins old chunks
// while sealing runs, so its Close trims many chunks without a new sync.
func TestLogTrimWaitsForDurableState(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14, EpochVersions: 2, RetainVersions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(1024)
	if err != nil {
		t.Fatal(err)
	}
	vw, err := db.View(db.Version())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		db.Write(r, 0, []byte{byte(i)})
		if _, _, err = db.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = db.Seal(); err != nil {
			t.Fatal(err)
		}
		if i == 20 {
			vw.Close()
		}
		db.mu.Lock()
		first, durable, n := db.chunks[0].epoch, db.durableEpoch, len(db.chunks)
		db.mu.Unlock()
		if first > durable {
			t.Fatalf("block %d: oldest kept chunk %d is newer than durable chunk %d", i, first, durable)
		}
		if i > 21 && n > 3 {
			t.Fatalf("block %d: %d chunks kept", i, n)
		}
	}
}
