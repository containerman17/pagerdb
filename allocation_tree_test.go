package pagerdb

import (
	"crypto/sha256"
	"testing"
)

func TestTreeTiles(t *testing.T) {
	for depth := 1; depth <= 17; depth++ {
		n := uint64(1) << depth
		seen := make(map[uint64]bool)
		for i := uint64(1); i < 2*n; i++ {
			off := treeOffset(i, depth)
			if off%32 != 0 || off+32 > treeStorageSize(n) || seen[off] {
				t.Fatalf("depth %d node %d offset %d", depth, i, off)
			}
			seen[off] = true
		}
	}
}

func TestTreeRootMatchesHeap(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(4096)
	if err != nil {
		t.Fatal(err)
	}
	db.Write(r, 17, []byte("tile root equality"))
	root, _, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	hashes := make([][32]byte, db.maxLeaves)
	var zero [LeafSize]byte
	for i := range hashes {
		if (i+1)*LeafSize <= len(db.state) {
			hashes[i] = sha256.Sum256(db.state[i*LeafSize : (i+1)*LeafSize])
		} else {
			hashes[i] = sha256.Sum256(zero[:])
		}
	}
	for len(hashes) > 1 {
		for i := 0; i < len(hashes)/2; i++ {
			hashes[i] = hashPair(hashes[2*i][:], hashes[2*i+1][:])
		}
		hashes = hashes[:len(hashes)/2]
	}
	if root != hashes[0] {
		t.Fatal("root differs from heap tree")
	}
}

func TestEmptySmallSegmentReturnsToPool(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a, err := db.Alloc(1024)
	if err != nil {
		t.Fatal(err)
	}
	b, err := db.Alloc(1024)
	if err != nil {
		t.Fatal(err)
	}
	seg := db.loc(a).off / SegSize
	if err = db.Free(a); err != nil {
		t.Fatal(err)
	}
	if db.hdr(hFreeSeg) == seg {
		t.Fatal("segment freed with live slot")
	}
	if err = db.Free(b); err != nil {
		t.Fatal(err)
	}
	if db.hdr(hFreeSeg) != seg {
		t.Fatal("empty segment not returned")
	}
	r, err := db.Alloc(SegSize)
	if err != nil {
		t.Fatal(err)
	}
	if db.loc(r).segs[0] != seg {
		t.Fatal("segment not reused by large region")
	}
}

func TestRegionTableBeyondOldLimit(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var r Region
	for range 1100 {
		r, err = db.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
	}
	if uint64(r) <= db.maxLeaves/16 {
		t.Fatal("did not exceed old region limit")
	}
	db.Write(r, 0, []byte("last"))
	if _, _, err = db.Commit(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	db.Read(r, 0, 4, buf)
	if string(buf) != "last" {
		t.Fatal(string(buf))
	}
}

func BenchmarkTreePath(b *testing.B) {
	for _, tiled := range []bool{false, true} {
		name := "Heap"
		if tiled {
			name = "Tiles"
		}
		b.Run(name, func(b *testing.B) {
			benchmarkTreePath(b, tiled)
		})
	}
}

func benchmarkTreePath(b *testing.B, tiled bool) {
	const depth = 24
	n := uint64(1) << depth
	tree := make([]byte, max(treeStorageSize(n), 2*n*32))
	var sum byte
	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		i := n + (uint64(k) * 11400714819323198485 & (n - 1))
		for i > 0 {
			off := i * 32
			if tiled {
				off = treeOffset(i, depth)
			}
			sum += tree[off]
			i >>= 1
		}
	}
	b.StopTimer()
	if sum != 0 {
		b.Fatal(sum)
	}
}
