package pagerdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
)

func writeXORHistoryBlock(db *DB, r Region, state []byte, value uint64) {
	var counter [32]byte
	binary.BigEndian.PutUint64(counter[24:], value)
	db.Write(r, 0, counter[:])
	copy(state, counter[:])
	// A crossing write, then an overlapping write in the same committed version.
	crossing := bytes.Repeat([]byte{byte(value)}, 200)
	db.Write(r, 900, crossing)
	copy(state[900:], crossing)
	counter[31] ^= 91
	db.Write(r, 1000, counter[:])
	copy(state[1000:], counter[:])
	db.Write(r, 2*LeafSize, counter[:])
	copy(state[2*LeafSize:], counter[:])
	// This no-op must not produce a backward change that alters reconstruction.
	db.Write(r, 3*LeafSize, state[3*LeafSize:3*LeafSize+64])
}

func checkXORHistory(t *testing.T, db *DB, r Region, snaps map[uint64][]byte, versions ...uint64) {
	t.Helper()
	for _, v := range versions {
		vw, err := db.View(v)
		if err != nil {
			t.Fatalf("view %d: %v", v, err)
		}
		got, err := vw.Copy(r)
		if err != nil {
			vw.Close()
			t.Fatal(err)
		}
		if !bytes.Equal(got, snaps[v]) {
			vw.Close()
			t.Fatalf("version %d bytes differ", v)
		}
		var rangeBytes [173]byte
		if err = vw.Read(r, 977, len(rangeBytes), rangeBytes[:]); err != nil {
			vw.Close()
			t.Fatal(err)
		}
		if !bytes.Equal(rangeBytes[:], snaps[v][977:1150]) {
			vw.Close()
			t.Fatalf("version %d crossing range differs", v)
		}
		vw.Close()
	}
}

func TestXORHistoryAfterLivePruning(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxLeaves: 1 << 14, EpochVersions: 2, RetainVersions: 3}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.Close() }()
	r, err := db.Alloc(4 * LeafSize)
	if err != nil {
		t.Fatal(err)
	}
	state := make([]byte, 4*LeafSize)
	snaps := map[uint64][]byte{}
	roots := map[uint64][32]byte{}
	for i := uint64(1); i <= 24; i++ {
		writeXORHistoryBlock(db, r, state, i)
		root, v, e := db.Commit()
		if e != nil {
			t.Fatal(e)
		}
		snaps[v] = bytes.Clone(state)
		roots[v] = root
	}
	if err = db.Seal(); err != nil {
		t.Fatal(err)
	}
	if db.chunks[0].start <= 2 {
		t.Fatal("test did not prune old live logs")
	}
	mark := db.Mark()
	db.Write(r, 0, []byte("pending head write"))
	checkXORHistory(t, db, r, snaps, 1, 2, 7, 13, 21, 23, 24)
	db.Undo(mark)
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	checkXORHistory(t, db, r, snaps, 1, 7, 13, 23, 24)
	// Old maps and exports must use the same backward walk as Read.
	for _, v := range []uint64{1, 13, 23} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			vw, e := db.View(v)
			if e != nil {
				t.Fatal(e)
			}
			defer vw.Close()
			mapped, e := vw.Map(r)
			if e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(mapped, snaps[v]) {
				t.Fatal("old mapping differs")
			}
			mapped[1000] ^= 255
			got, e := vw.Copy(r)
			if e != nil || !bytes.Equal(got, snaps[v]) {
				t.Fatalf("private write changed view: %v", e)
			}
			var image bytes.Buffer
			if e = db.Export(v, &image); e != nil {
				t.Fatal(e)
			}
			imported, e := Import(filepath.Join(t.TempDir(), "imported"), bytes.NewReader(image.Bytes()), roots[v])
			if e != nil {
				t.Fatal(e)
			}
			defer imported.Close()
			checkXORHistory(t, imported, r, snaps, v)
		})
	}
}

func TestXORHistoryRewindRewrite(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxLeaves: 1 << 14, EpochVersions: 2, RetainVersions: 8, Sync: true}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.Close() }()
	r, err := db.Alloc(4 * LeafSize)
	if err != nil {
		t.Fatal(err)
	}
	state := make([]byte, 4*LeafSize)
	snaps := map[uint64][]byte{}
	roots := map[uint64][32]byte{}
	for i := uint64(1); i <= 12; i++ {
		writeXORHistoryBlock(db, r, state, i)
		root, v, e := db.Commit()
		if e != nil {
			t.Fatal(e)
		}
		snaps[v] = bytes.Clone(state)
		roots[v] = root
	}
	if err = db.Seal(); err != nil {
		t.Fatal(err)
	}
	if db.chunks[0].start == 0 {
		t.Fatal("test did not prune a live epoch")
	}
	held, err := db.View(5)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Rewind(5); err != nil {
		t.Fatal(err)
	}
	if db.Root() != roots[5] {
		t.Fatal("rewind root differs")
	}
	heldBytes, err := held.Copy(r)
	if err != nil || !bytes.Equal(heldBytes, snaps[5]) {
		t.Fatalf("held view after its epoch was removed: %v", err)
	}
	held.Close()
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	checkXORHistory(t, db, r, snaps, 1, 3, 5)
	state = bytes.Clone(snaps[5])
	for i := uint64(6); i <= 18; i++ {
		writeXORHistoryBlock(db, r, state, 1000+i)
		root, v, e := db.Commit()
		if e != nil {
			t.Fatal(e)
		}
		snaps[v] = bytes.Clone(state)
		roots[v] = root
	}
	if err = db.Seal(); err != nil {
		t.Fatal(err)
	}
	checkXORHistory(t, db, r, snaps, 1, 3, 5, 6, 9, 17, 18)
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	checkXORHistory(t, db, r, snaps, 1, 3, 5, 6, 9, 17, 18)
}

func TestXORHistoryDropOldestEpochs(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxLeaves: 1 << 14, EpochVersions: 2, RetainVersions: 3}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Alloc(4 * LeafSize)
	if err != nil {
		t.Fatal(err)
	}
	state := make([]byte, 4*LeafSize)
	snaps := map[uint64][]byte{}
	roots := map[uint64][32]byte{}
	for i := uint64(1); i <= 24; i++ {
		writeXORHistoryBlock(db, r, state, i)
		root, v, e := db.Commit()
		if e != nil {
			t.Fatal(e)
		}
		snaps[v] = bytes.Clone(state)
		roots[v] = root
	}
	if err = db.Seal(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := NewLocalStore(filepath.Join(dir, "epochs"))
	if err != nil {
		t.Fatal(err)
	}
	for epoch := uint64(1); epoch <= 4; epoch++ {
		if err = store.Delete(epoch); err != nil {
			t.Fatal(err)
		}
	}
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, v := range []uint64{0, 1, 7} {
		vw, e := db.View(v)
		if e == nil {
			vw.Close()
			t.Fatalf("dropped version %d remained readable", v)
		}
		if e != ErrVersion {
			t.Fatalf("dropped version %d: %v", v, e)
		}
	}
	checkXORHistory(t, db, r, snaps, 8, 9, 13, 21, 24)
	vw, err := db.ViewRoot(roots[8])
	if err != nil {
		t.Fatal(err)
	}
	if vw.Version() != 8 {
		t.Fatalf("boundary root opened version %d", vw.Version())
	}
	vw.Close()
	var image bytes.Buffer
	if err = db.Export(8, &image); err != nil {
		t.Fatal(err)
	}
	imported, err := Import(filepath.Join(t.TempDir(), "boundary"), bytes.NewReader(image.Bytes()), roots[8])
	if err != nil {
		t.Fatal(err)
	}
	checkXORHistory(t, imported, r, snaps, 8)
	imported.Close()
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	// An interior hole loses changes needed by the remaining older epochs.
	if err = store.Delete(7); err != nil {
		t.Fatal(err)
	}
	opened, e := Open(dir, opts)
	if e == nil {
		opened.Close()
		t.Fatal("accepted an interior sealed-history gap")
	}
}
