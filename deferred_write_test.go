package pagerdb

import (
	"bytes"
	"testing"
)

func TestDeferredWriteIndex(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(2 * LeafSize)
	if err != nil {
		t.Fatal(err)
	}
	_, v, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	indexed := db.entries.end
	db.Write(r, LeafSize-16, bytes.Repeat([]byte{1}, 32))
	db.Write(r, LeafSize-8, bytes.Repeat([]byte{2}, 32))
	if db.entries.end != indexed || len(db.pending) != 2 {
		t.Fatal("Write materialized the window index")
	}
	vw, err := db.View(v)
	if err != nil {
		t.Fatal(err)
	}
	defer vw.Close()
	got := make([]byte, 48)
	if err := vw.Read(r, LeafSize-16, len(got), got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, make([]byte, len(got))) {
		t.Fatal("view exposed pending writes")
	}
	mark := db.Mark()
	db.Write(r, LeafSize-16, bytes.Repeat([]byte{3}, 48))
	db.Undo(mark)
	_, v2, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 48)
	copy(want, bytes.Repeat([]byte{1}, 32))
	copy(want[8:], bytes.Repeat([]byte{2}, 32))
	db.Read(r, LeafSize-16, len(got), got)
	if !bytes.Equal(got, want) {
		t.Fatal("Undo lost overlapping writes")
	}
	if err := db.Seal(); err != nil {
		t.Fatal(err)
	}
	sealed, err := db.View(v2)
	if err != nil {
		t.Fatal(err)
	}
	defer sealed.Close()
	if err := sealed.Read(r, LeafSize-16, len(got), got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("sealed view lost overlapping writes")
	}
}

func TestDeferredWriteRewindEarlierChunk(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14, EpochVersions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(32)
	if err != nil {
		t.Fatal(err)
	}
	for i := byte(1); i <= 4; i++ {
		db.Write(r, 0, []byte{i})
		if _, _, err := db.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	db.Write(r, 0, []byte{9})
	if err := db.Rewind(1); err != nil {
		t.Fatal(err)
	}
	var got [1]byte
	db.Read(r, 0, 1, got[:])
	if got[0] != 1 {
		t.Fatalf("rewind got %d, want 1", got[0])
	}
}

func TestRepeatedWriteUndoMarks(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(32)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.Commit(); err != nil {
		t.Fatal(err)
	}
	db.Write(r, 0, []byte{1})
	tail := db.cur.tail
	db.Write(r, 0, []byte{2})
	if db.cur.tail != tail {
		t.Fatal("repeated range logged twice")
	}
	m := db.Mark()
	db.Write(r, 0, []byte{3})
	db.Write(r, 0, []byte{4})
	db.Undo(m)
	var got [1]byte
	db.Read(r, 0, 1, got[:])
	if got[0] != 2 {
		t.Fatalf("Undo got %d, want 2", got[0])
	}
	db.Write(r, 0, []byte{5})
	db.Undo(m)
	db.Read(r, 0, 1, got[:])
	if got[0] != 2 {
		t.Fatalf("repeated Undo got %d, want 2", got[0])
	}
}
