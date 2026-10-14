package pagerdb

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// naiveRoot hashes every leaf of the state file (read with os.ReadFile, zero padded to
// capacity) and folds the levels, independent of the incremental tree.
func naiveRoot(t testing.TB, dir string, maxLeaves uint64) [32]byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	full := make([]byte, maxLeaves*LeafSize)
	copy(full, b)
	level := make([][32]byte, maxLeaves)
	for i := range level {
		level[i] = sha256.Sum256(full[i*LeafSize : (i+1)*LeafSize])
	}
	for len(level) > 1 {
		next := make([][32]byte, len(level)/2)
		for i := range next {
			next[i] = hashPair(level[2*i][:], level[2*i+1][:])
		}
		level = next
	}
	return level[0]
}

// model is the expected content of every live region.
type model map[Region][]byte

func (m model) clone() model {
	c := model{}
	for r, b := range m {
		c[r] = bytes.Clone(b)
	}
	return c
}

func roundSize(n int) int {
	if c := classOf(uint64(n)); c != 0 {
		return int(classSize(c))
	}
	return (n + SegSize - 1) / SegSize * SegSize
}

var testSizes = []int{1, 100, 1024, 1500, 4096, 10000, 16 << 10, 100 << 10, 1 << 20, 1<<20 + 1, 3 << 20}

// step applies one random operation to db and to the model.
func step(t testing.TB, rng *rand.Rand, db *DB, m model, sizes []int) {
	t.Helper()
	regions := func() Region {
		keys := slices.Sorted(maps.Keys(m))
		return keys[rng.IntN(len(keys))]
	}
	switch op := rng.IntN(100); {
	case op < 8 || len(m) == 0:
		size := sizes[rng.IntN(len(sizes))]
		r, err := db.Alloc(size)
		if errors.Is(err, ErrFull) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, dup := m[r]; dup {
			t.Fatalf("Alloc returned live region %d", r)
		}
		m[r] = make([]byte, roundSize(size))
	case op < 12:
		r := regions()
		size := len(m[r]) + 1 + rng.IntN(2*len(m[r]))
		err := db.Grow(r, size)
		if errors.Is(err, ErrFull) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		g := make([]byte, roundSize(size))
		copy(g, m[r])
		m[r] = g
	case op < 16:
		r := regions()
		if err := db.Free(r); err != nil {
			t.Fatal(err)
		}
		delete(m, r)
	case op < 70:
		r := regions()
		n := 1 + rng.IntN(min(len(m[r]), 300))
		if rng.IntN(20) == 0 {
			n = 1 + rng.IntN(len(m[r]))
		}
		off := rng.IntN(len(m[r]) - n + 1)
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rng.Uint32())
		}
		db.Write(r, off, b)
		copy(m[r][off:], b)
	case op < 85:
		r := regions()
		n := 1 + rng.IntN(min(len(m[r]), 3000))
		off := rng.IntN(len(m[r]) - n + 1)
		db.Touch(r, off, n)
		mp, err := db.Map(r)
		if err != nil {
			t.Fatal(err)
		}
		if len(mp) != len(m[r]) {
			t.Fatalf("Map len %d, want %d", len(mp), len(m[r]))
		}
		for i := off; i < off+n; i++ {
			mp[i] = byte(rng.Uint32())
		}
		copy(m[r][off:], mp[off:off+n])
	default:
		mark := db.Mark()
		saved := m.clone()
		for range 1 + rng.IntN(5) {
			step(t, rng, db, m, sizes)
		}
		db.Undo(mark)
		clear(m)
		maps.Copy(m, saved)
	}
}

func checkModel(t testing.TB, db *DB, m model) {
	t.Helper()
	for r, want := range m {
		got := make([]byte, len(want))
		db.Read(r, 0, len(want), got)
		if !bytes.Equal(got, want) {
			t.Fatalf("version %d region %d: bytes differ", db.Version(), r)
		}
	}
}

func TestModel(t *testing.T) {
	t.Run("NoSync", func(t *testing.T) { testModel(t, false) })
	// Sync stages Write bytes until the block's barrier; reads must still see them.
	t.Run("Sync", func(t *testing.T) { testModel(t, true) })
}

func testModel(t *testing.T, syncMode bool) {
	dir := t.TempDir()
	const maxLeaves = 1 << 15
	db, err := Open(dir, Options{MaxLeaves: maxLeaves, Sync: syncMode})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rng := rand.New(rand.NewPCG(1, 2))
	m := model{}
	for block := 0; block < 150; block++ {
		for range rng.IntN(40) {
			step(t, rng, db, m, testSizes)
		}
		checkModel(t, db, m)
		root, ver, err := db.Commit()
		if err != nil {
			t.Fatal(err)
		}
		if ver != uint64(block+1) {
			t.Fatalf("version %d, want %d", ver, block+1)
		}
		checkModel(t, db, m)
		if want := naiveRoot(t, dir, maxLeaves); root != want {
			t.Fatalf("version %d: root %x, naive %x", ver, root, want)
		}
	}
	large := 0
	for r := range m {
		if len(m[r]) > 1<<20 {
			large++
		}
	}
	t.Logf("regions %d (large %d), segments %d, log entries %d", len(m), large, db.hdr(hNextSeg), db.seq())
}
