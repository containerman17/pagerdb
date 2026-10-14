package pagerdb

import (
	"bytes"
	"errors"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestRewind(t *testing.T) {
	o := Options{MaxLeaves: 1 << 14}
	roots, snaps := reference(t, o, 11, 20)
	dir := t.TempDir()
	db, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.Close() }()
	workload(t, db, 11, 20, func(uint64, [32]byte, model) {})
	vw, err := db.View(15)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Rewind(12); err == nil {
		t.Fatal("rewind below an open view succeeded")
	}
	vw.Close()
	db.Write(slices.Min(slices.Collect(maps.Keys(snaps[20]))), 0, []byte("pending")) // uncommitted writes are rewound too
	if err := db.Rewind(12); err != nil {
		t.Fatal(err)
	}
	if db.Version() != 12 || db.Root() != roots[12] {
		t.Fatalf("after rewind: version %d", db.Version())
	}
	checkModel(t, db, snaps[12])
	if root := naiveRoot(t, dir, o.MaxLeaves); root != roots[12] {
		t.Fatal("naive root differs after rewind")
	}
	// Rewind survives a restart and the head moves on from there.
	db.Close()
	if db, err = Open(dir, o); err != nil {
		t.Fatal(err)
	}
	if db.Version() != 12 || db.Root() != roots[12] {
		t.Fatalf("reopen after rewind: version %d", db.Version())
	}
	checkModel(t, db, snaps[12])
	if err := db.Rewind(3); err != nil {
		t.Fatal(err)
	}
	checkModel(t, db, snaps[3])
	db.Write(slices.Min(slices.Collect(maps.Keys(snaps[3]))), 0, []byte("new history"))
	if _, v, err := db.Commit(); err != nil || v != 4 {
		t.Fatalf("commit after rewind: version %d, %v", v, err)
	}
	if db.Root() == roots[4] {
		t.Fatal("new history has the old root")
	}
	if err := db.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := db.Rewind(3); err != nil {
		t.Fatalf("rewind across a sealed epoch: %v", err)
	}
}

func TestProof(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var m model
	workload(t, db, 13, 10, func(_ uint64, _ [32]byte, mm model) { m = mm })
	rng := rand.New(rand.NewPCG(1, 1))
	for r, b := range m {
		off := rng.IntN(len(b))
		p, err := db.Proof(r, off)
		if err != nil {
			t.Fatal(err)
		}
		if p.Data[off%LeafSize] != b[off] || len(p.Siblings) != 14 {
			t.Fatal("proof leaf does not hold the region byte")
		}
		if !Verify(db.Root(), p) {
			t.Fatalf("region %d offset %d: proof does not verify", r, off)
		}
		bad := p
		bad.Data[rng.IntN(LeafSize)] ^= 1
		if Verify(db.Root(), bad) {
			t.Fatal("tampered leaf verifies")
		}
		bad = p
		bad.Siblings = append([][32]byte(nil), p.Siblings...)
		bad.Siblings[rng.IntN(len(bad.Siblings))][0] ^= 1
		if Verify(db.Root(), bad) {
			t.Fatal("tampered sibling verifies")
		}
		bad = p
		// Equal sibling subtrees cannot distinguish their left/right positions.
		// Flip the first direction whose two subtree hashes differ.
		h := sha256Sum(p.Data[:])
		for level, sibling := range p.Siblings {
			if h != sibling {
				bad.Leaf ^= uint64(1) << level
				if Verify(db.Root(), bad) {
					t.Fatal("wrong leaf index verifies")
				}
				break
			}
			h = hashPair(h[:], sibling[:])
		}
	}
	for r := range m {
		db.Write(r, 0, []byte{1})
		if _, err := db.Proof(r, 0); !errors.Is(err, ErrPending) {
			t.Fatalf("proof with pending writes: %v", err)
		}
		break
	}
}

func TestLargeRegion(t *testing.T) {
	dir := t.TempDir()
	const maxLeaves = 1 << 15
	db, err := Open(dir, Options{MaxLeaves: maxLeaves})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a, err := db.Alloc(1<<20 + 1) // one 2 MiB segment
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Alloc(100); err != nil { // takes the next segment, so a's growth is not adjacent
		t.Fatal(err)
	}
	if err := db.Grow(a, 5<<20); err != nil {
		t.Fatal(err)
	}
	segs := db.loc(a).segs
	if len(segs) != 3 || segs[1] == segs[0]+1 {
		t.Fatalf("segments %v: want 3, not contiguous in the file", segs)
	}
	m, err := db.Map(a)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 6<<20 {
		t.Fatalf("Map len %d", len(m))
	}
	want := make([]byte, len(m))
	rng := rand.New(rand.NewPCG(2, 2))
	// Direct writes across both segment boundaries through the contiguous mapping.
	for _, at := range []int{SegSize - 300, 2*SegSize - 5} {
		db.Touch(a, at, 1000)
		for i := at; i < at+1000; i++ {
			m[i] = byte(rng.Uint32())
			want[i] = m[i]
		}
	}
	b := make([]byte, 5000)
	for i := range b {
		b[i] = byte(i)
	}
	db.Write(a, 4*SegSize/2-2500, b)
	copy(want[4*SegSize/2-2500:], b)
	got := make([]byte, len(want))
	db.Read(a, 0, len(got), got)
	if !bytes.Equal(got, want) {
		t.Fatal("Read differs from writes through Map and Write")
	}
	root, _, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if root != naiveRoot(t, dir, maxLeaves) {
		t.Fatal("root differs from naive tree")
	}
	// Grow by one more segment: no copy, contents kept, a new contiguous mapping.
	if err := db.Grow(a, 7<<20); err != nil {
		t.Fatal(err)
	}
	if s := db.loc(a).segs; len(s) != 4 || s[0] != segs[0] || s[2] != segs[2] {
		t.Fatalf("grow moved segments: %v -> %v", segs, s)
	}
	m, err = db.Map(a)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 8<<20 || !bytes.Equal(m[:len(want)], want) || m[len(m)-1] != 0 {
		t.Fatal("contents after grow differ")
	}
	db.Touch(a, len(m)-10, 10)
	copy(m[len(m)-10:], "tail bytes")
	if root, _, err = db.Commit(); err != nil {
		t.Fatal(err)
	}
	if root != naiveRoot(t, dir, maxLeaves) {
		t.Fatal("root differs from naive tree after grow")
	}
	vw, err := db.View(1)
	if err != nil {
		t.Fatal(err)
	}
	old, err := vw.Copy(a)
	vw.Close()
	if err != nil || !bytes.Equal(old, want) {
		t.Fatalf("view at version 1: %v", err)
	}
	// Free returns zeroed segments; the next large region reuses them.
	if err := db.Free(a); err != nil {
		t.Fatal(err)
	}
	c, err := db.Alloc(3 << 20)
	if err != nil {
		t.Fatal(err)
	}
	m, _ = db.Map(c)
	if !bytes.Equal(m, make([]byte, len(m))) {
		t.Fatal("reused segments are not zero")
	}
	if root, _, err = db.Commit(); err != nil || root != naiveRoot(t, dir, maxLeaves) {
		t.Fatalf("root after free and realloc: %v", err)
	}
}

// TestConcurrentViews reads pinned views from other goroutines while the head writes and commits.
func TestConcurrentViews(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14, EpochVersions: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(64 << 10)
	if err != nil {
		t.Fatal(err)
	}
	snaps := map[uint64][]byte{}
	cur := make([]byte, 64<<10)
	rng := rand.New(rand.NewPCG(4, 4))
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for range 40 {
		for range 50 {
			off := rng.IntN(len(cur) - 64)
			b := make([]byte, 64)
			for i := range b {
				b[i] = byte(rng.Uint32())
			}
			db.Write(r, off, b)
			copy(cur[off:], b)
		}
		_, v, err := db.Commit()
		if err != nil {
			t.Fatal(err)
		}
		snaps[v] = bytes.Clone(cur)
		vw, err := db.View(v)
		if err != nil {
			t.Fatal(err)
		}
		want := snaps[v]
		wg.Go(func() {
			defer vw.Close()
			for range 20 {
				got, err := vw.Copy(r)
				if err != nil || !bytes.Equal(got, want) {
					errs <- errors.Join(err, errors.New("concurrent view differs"))
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestProofAfterUndo(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(1024)
	if err != nil {
		t.Fatal(err)
	}
	db.Write(r, 0, []byte("committed"))
	root, v, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	m := db.Mark()
	db.Write(r, 0, []byte("pending"))
	db.Undo(m)
	p, err := db.Proof(r, 0)
	if err != nil || !Verify(root, p) {
		t.Fatal("proof after full undo", err)
	}
	if db.Version() != v || db.Root() != root {
		t.Fatal("proof changed committed version")
	}
}

// TestCleanOpenAndVerify checks that a clean Open trusts the stored tree, that
// Verify finds a tree node that disagrees with state, and that an unclean Open
// rebuilds the tree.
func TestCleanOpenAndVerify(t *testing.T) {
	dir := t.TempDir()
	o := Options{MaxLeaves: 1 << 14}
	db, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Alloc(3 << 20)
	if err != nil {
		t.Fatal(err)
	}
	db.Write(r, 5000, []byte("leaf"))
	root, v, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Verify(); err != nil {
		t.Fatal(err)
	}
	db.Write(r, 0, []byte{1})
	if err = db.Verify(); err != ErrPending {
		t.Fatalf("Verify with pending writes: %v", err)
	}
	seg := db.loc(r).segs[0]
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	// Corrupt a leaf hash in the tree file; the root node stays intact.
	tree, err := os.OpenFile(filepath.Join(dir, "tree"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	leaf := seg*SegSize/LeafSize + 4
	if _, err = tree.WriteAt([]byte{0xff}, int64(treeOffset(db.maxLeaves+leaf, db.depth))); err != nil {
		t.Fatal(err)
	}
	tree.Close()
	db, err = Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	if db.Root() != root || db.Version() != v {
		t.Fatal("clean open changed the head")
	}
	if err = db.Verify(); err == nil || !strings.Contains(err.Error(), "tree node") {
		t.Fatalf("Verify after a clean open over a corrupt tree: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	// A damaged marker forces the full rebuild, which repairs the tree.
	if err = os.WriteFile(filepath.Join(dir, "clean"), []byte("PGCLEAN1 damaged"), 0o644); err != nil {
		t.Fatal(err)
	}
	if db, err = Open(dir, o); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if !db.unclean || db.Root() != root {
		t.Fatal("damaged marker did not take the verified path")
	}
	if err = db.Verify(); err != nil {
		t.Fatal(err)
	}
	// Verify detects state that no longer hashes to the committed root.
	off := seg*SegSize + 5000
	db.state[off] ^= 1
	if err = db.Verify(); err == nil {
		t.Fatal("Verify accepted corrupt state")
	}
	db.state[off] ^= 1
}
