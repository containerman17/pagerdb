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
	oldBase := db.entries.base
	if db.chunks[0].start != 0 {
		t.Fatal("open view did not pin its log")
	}
	var value [1]byte
	if err = pinned.Read(r, 0, 1, value[:]); err != nil || value[0] != 1 {
		t.Fatalf("pinned read: %v %v", value, err)
	}
	pinned.Close()
	if db.entries.base == oldBase {
		t.Fatal("closing pin did not prune retained history")
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
	if db.entries.len() != 4*4 {
		t.Fatalf("index holds %d entries, want %d", db.entries.len(), 4*4)
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

func TestSeqBufPages(t *testing.T) {
	var b seqBuf[int64]
	const page = 1 << seqPageBits
	next := int64(0)
	for round := range 6 {
		for range page/2 + 17 {
			b.push(next * 3)
			next++
		}
		if round%2 == 1 {
			b.end -= 5 // undo pops from the back, then pushes reuse the space
			next -= 5
		}
		b.cut(b.end - page/3)
		for s := b.base; s < b.end; s++ {
			if *b.at(s) != s*3 {
				t.Fatalf("round %d: seq %d holds %d", round, s, *b.at(s))
			}
		}
		if n := int64(len(b.pages)); n > b.len()/page+2 {
			t.Fatalf("round %d: %d pages for %d live values", round, n, b.len())
		}
	}
	b.cut(b.end)
	if len(b.pages) > 1 {
		t.Fatalf("%d pages left after cutting everything", len(b.pages))
	}
	b.push(next * 3)
	if *b.at(next) != next*3 {
		t.Fatal("push after cutting everything")
	}
}
