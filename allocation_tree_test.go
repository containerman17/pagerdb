package pagerdb

import (
	"crypto/sha256"
	"golang.org/x/sys/unix"
	"math/bits"
	"math/rand/v2"
	"os"
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

// TestTreeTablesMatchFormula checks the precomputed tables and the path
// iterator against the layout formula documented in the README.
func TestTreeTablesMatchFormula(t *testing.T) {
	formula := func(i uint64, depth int) uint64 {
		d := bits.Len64(i) - 1
		top := depth%7 + 1
		if d < top {
			return (i - 1) * 32
		}
		k := (d - top) / 7
		rootDepth := top + 7*k
		localDepth := d - rootDepth
		prefix := i >> localDepth
		before := uint64(1) + (uint64(1)<<top)*((uint64(1)<<(7*k))-1)/127
		tile := before + prefix - (uint64(1) << rootDepth)
		local := (uint64(1) << localDepth) + (i & ((uint64(1) << localDepth) - 1))
		return tile*4096 + (local-1)*32
	}
	rng := rand.New(rand.NewPCG(5, 6))
	for depth := 1; depth <= 32; depth++ {
		n := uint64(1) << depth
		for range 2000 {
			leaf := n + rng.Uint64N(n)
			var walk []uint64
			for i := leaf; i > 0; {
				local, start := tileStart(i, depth)
				for ; local > 0; local >>= 1 {
					walk = append(walk, start+(local-1)*32)
					i >>= 1
				}
			}
			for i, w := leaf, walk; i > 0; i, w = i>>1, w[1:] {
				if got, want := treeOffset(i, depth), formula(i, depth); got != want || w[0] != want {
					t.Fatalf("depth %d node %d: table %d walk %d formula %d", depth, i, got, w[0], want)
				}
			}
		}
	}
}

func TestTreePathUsesFourTiles(t *testing.T) {
	for leaf := uint64(0); leaf < 1<<24; leaf += 7919 {
		pages := make(map[uint64]bool)
		for i := (uint64(1) << 24) + leaf; i > 0; i >>= 1 {
			pages[treeOffset(i, 24)/4096] = true
		}
		if len(pages) != 4 {
			t.Fatalf("leaf %d uses %d tiles", leaf, len(pages))
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
		if uint64((i+1)*LeafSize) <= db.hdr(hNextSeg)*SegSize {
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

// BenchmarkTreePath walks leaf-to-root paths. Random spreads paths over a
// 2^24-leaf tree (1 GiB of nodes, warm pages); Hot repeats 256 cached paths.
func BenchmarkTreePath(b *testing.B) {
	for _, hot := range []bool{false, true} {
		for _, tiled := range []bool{false, true} {
			name := map[bool]string{false: "Random", true: "Hot"}[hot] + map[bool]string{false: "Heap", true: "Tiles"}[tiled]
			b.Run(name, func(b *testing.B) { benchmarkTreePath(b, tiled, hot) })
		}
	}
}

func benchmarkTreePath(b *testing.B, tiled, hot bool) {
	const depth = 24
	n := uint64(1) << depth
	tree := make([]byte, max(treeStorageSize(n), 2*n*32))
	for i := 0; i < len(tree); i += 4096 {
		tree[i] = 1 // fault every page in before timing
		tree[i] = 0
	}
	mask := n - 1
	if hot {
		mask = 255
	}
	var sum byte
	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		i := n + (uint64(k)&mask*11400714819323198485)&(n-1)
		if tiled {
			for i > 0 {
				local, start := tileStart(i, depth)
				for ; local > 0; local >>= 1 {
					sum += tree[start+(local-1)*32]
					i >>= 1
				}
			}
			continue
		}
		for ; i > 0; i >>= 1 {
			sum += tree[i*32]
		}
	}
	b.StopTimer()
	if sum != 0 {
		b.Fatal(sum)
	}
}

func TestRegionTableAddsSegment(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 18})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Allocate identifiers directly to isolate the table growth boundary.
	var r Region
	for range regionsPerSegment {
		r, err = db.allocID()
		if err != nil {
			t.Fatal(err)
		}
	}
	if uint64(r) != regionsPerSegment {
		t.Fatal(r)
	}
	first, second := db.hdr(db.regTab), db.hdr(db.regTab+8)
	if first == 0 || second == 0 || first == second {
		t.Fatalf("table segments %d %d", first, second)
	}
	if db.regionOff(r) != second*SegSize {
		t.Fatal("second table segment offset")
	}
}

func BenchmarkTreeColdPath(b *testing.B) {
	for _, tiled := range []bool{false, true} {
		name := "Heap"
		if tiled {
			name = "Tiles"
		}
		b.Run(name, func(b *testing.B) {
			const depth = 24
			n := uint64(1) << depth
			f, err := os.CreateTemp(b.TempDir(), "tree")
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()
			size := 2 * n * 32
			if tiled {
				size = treeStorageSize(n)
			}
			if err = f.Truncate(int64(size)); err != nil {
				b.Fatal(err)
			}
			tree, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
			if err != nil {
				b.Fatal(err)
			}
			defer unix.Munmap(tree)
			var sum byte
			b.ResetTimer()
			for k := 0; k < b.N; k++ {
				b.StopTimer()
				if err = unix.Madvise(tree, unix.MADV_DONTNEED); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				for i := n + (uint64(k) * 11400714819323198485 & (n - 1)); i > 0; i >>= 1 {
					off := i * 32
					if tiled {
						off = treeOffset(i, depth)
					}
					sum += tree[off]
				}
			}
			b.StopTimer()
			if sum != 0 {
				b.Fatal(sum)
			}
		})
	}
}

func TestMinimumCapacityAllocates(t *testing.T) {
	if db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 12}); err == nil {
		db.Close()
		t.Fatal("accepted capacity without space for a region")
	}
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 13})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Alloc(1); err != nil {
		t.Fatal(err)
	}
	if _, _, err = db.Commit(); err != nil {
		t.Fatal(err)
	}
}
