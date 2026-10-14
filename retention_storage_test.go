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
	for _, e := range oldEntries[:removed] {
		if e.c != nil {
			t.Fatal("discarded entry retains chunk pointer")
		}
	}
	if len(db.entries) > 0 && &db.entries[0] != &oldEntries[removed] {
		t.Fatal("pruning copied retained entries")
	}
	tail := db.entries
	if err = db.Rewind(9); err != nil {
		t.Fatal(err)
	}
	for _, e := range tail[len(db.entries):] {
		if e.c != nil {
			t.Fatal("rewind retains discarded chunk pointer")
		}
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
