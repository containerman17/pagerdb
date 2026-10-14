package pagerdb

import "testing"

func TestRetentionStoragePinAndRewind(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14, EpochVersions: 2, RetainVersions: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(1024)
	if err != nil {
		t.Fatal(err)
	}
	db.Write(r, 0, []byte{1})
	if _, _, err = db.Commit(); err != nil {
		t.Fatal(err)
	}
	pinned, err := db.View(1)
	if err != nil {
		t.Fatal(err)
	}
	for v := byte(2); v <= 12; v++ {
		db.Write(r, 0, []byte{v})
		if _, _, err = db.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	db.sealWG.Wait()
	if db.sealErr != nil {
		t.Fatal(db.sealErr)
	}
	// Retain the backing storage to check that pruning releases chunk pointers.
	oldEntries := db.entries
	oldBase := db.entryBase
	if db.chunks[0].start != 0 {
		t.Fatal("open view did not pin its log")
	}
	var value [1]byte
	if err = pinned.Read(r, 0, 1, value[:]); err != nil || value[0] != 1 {
		t.Fatalf("pinned read: %v %v", value, err)
	}
	pinned.Close()
	removed := int(db.entryBase - oldBase)
	if removed == 0 {
		t.Fatal("closing pin did not prune retained history")
	}
	if len(db.entries) > 0 && &db.entries[0] != &oldEntries[removed] {
		t.Fatal("pruning copied retained entries")
	}
	if err = db.Rewind(9); err != nil {
		t.Fatal(err)
	}
	for v := byte(10); v <= 18; v++ {
		db.Write(r, 0, []byte{v + 80})
		if _, _, err = db.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	db.sealWG.Wait()
	for _, v := range []uint64{9, 14, 18} {
		view, e := db.View(v)
		if e != nil {
			t.Fatal(e)
		}
		e = view.Read(r, 0, 1, value[:])
		view.Close()
		want := byte(v)
		if v > 9 {
			want += 80
		}
		if e != nil || value[0] != want {
			t.Fatalf("version %d: %v %v", v, value, e)
		}
	}
}

// TestIndexFollowsRetention checks that the window index keeps about
// RetainVersions blocks inside a long epoch, and that pruned versions become
// readable again once their epoch is sealed.
func TestIndexFollowsRetention(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14, RetainVersions: 4, EpochVersions: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(4 * LeafSize)
	if err != nil {
		t.Fatal(err)
	}
	for v := 1; v <= 30; v++ {
		for leaf := range 4 {
			db.Write(r, leaf*LeafSize+v%7, []byte{byte(v)})
		}
		if _, _, err = db.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if len(db.entries) != 4*4 {
		t.Fatalf("index holds %d entries, want %d", len(db.entries), 4*4)
	}
	check := func(vw *View, v int) {
		b := make([]byte, 1)
		if err := vw.Read(r, 3*LeafSize+v%7, 1, b); err != nil || b[0] != byte(v) {
			t.Fatalf("version %d: %v %v", v, b, err)
		}
	}
	vw, err := db.View(db.Version() - 4)
	if err != nil {
		t.Fatal(err)
	}
	check(vw, int(db.Version()-4))
	vw.Close()
	if _, err = db.View(10); err != ErrWindow {
		t.Fatalf("pruned unsealed version: %v", err)
	}
	if err = db.Seal(); err != nil {
		t.Fatal(err)
	}
	if vw, err = db.View(10); err != nil {
		t.Fatal(err)
	}
	defer vw.Close()
	check(vw, 10)
}
