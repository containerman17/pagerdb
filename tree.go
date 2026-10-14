package pagerdb

import (
	"crypto/sha256"
	"errors"
	"hash/crc32"
	"math/bits"
	"runtime"
	"slices"
	"sync"
)

func sha256Sum(b []byte) [32]byte { return sha256.Sum256(b) }

func hashPair(a, b []byte) [32]byte {
	var buf [64]byte
	copy(buf[:32], a)
	copy(buf[32:], b)
	return sha256.Sum256(buf[:])
}

func (db *DB) markDirty(off, n uint64) {
	if n == 0 {
		return
	}
	for l := off / LeafSize; l <= (off+n-1)/LeafSize; l++ {
		if db.dirtyBit[l/64]&(1<<(l%64)) == 0 {
			db.dirtyBit[l/64] |= 1 << (l % 64)
			db.dirty = append(db.dirty, uint32(l))
		}
	}
}

// treeOffset groups seven tree levels into each 4 KiB tile. The top tile
// contains the remainder so every lower tile has all 127 nodes.
func treeOffset(i uint64, depth int) uint64 {
	t := &tileTables[depth][bits.Len64(i)-1]
	return t.base + (i>>t.shift)*4096 + (i&t.mask)*32
}

// tileLevel turns treeOffset into a shift, a mask and an add for one node depth.
type tileLevel struct{ shift, mask, base uint64 }

// tileTables[depth][d] holds the precomputed layout of nodes at heap depth d.
// Bases rely on unsigned wraparound; the sums are always in range.
var tileTables = func() (t [33][33]tileLevel) {
	for depth := range 33 {
		top := depth%7 + 1
		for d := 0; d <= depth; d++ {
			if d < top {
				// Tile 0: offset (i-1)*32, with i>>d == 1 contributing one tile.
				t[depth][d] = tileLevel{uint64(d), 1<<d - 1, (1<<d-1)*32 - 4096}
				continue
			}
			k := (d - top) / 7
			rootDepth := top + 7*k
			local := uint64(d - rootDepth)
			before := uint64(1) + (uint64(1)<<top)*((uint64(1)<<(7*k))-1)/127
			t[depth][d] = tileLevel{local, 1<<local - 1, (before-uint64(1)<<rootDepth)*4096 + (1<<local-1)*32}
		}
	}
	return t
}()

// tileStart returns node i's 1-based index inside its tile and the tile's byte
// offset. Ancestors inside the same tile are local>>1, local>>2, ... down to 1,
// so a leaf-to-root walk consults the table once per tile.
func tileStart(i uint64, depth int) (local, start uint64) {
	t := &tileTables[depth][bits.Len64(i)-1]
	return uint64(1)<<t.shift | i&t.mask, t.base + (i>>t.shift)*4096 - (uint64(1)<<t.shift-1)*32
}

func treeStorageSize(leaves uint64) uint64 {
	depth := bits.TrailingZeros64(leaves)
	return (treeOffset(2*leaves-1, depth)/4096 + 1) * 4096
}

// node returns tree node i at level (0 = leaf). An all-zero slot is an untouched empty subtree.
func (db *DB) node(i uint64, level int) []byte { return db.nodeAt(treeOffset(i, db.depth), level) }

func (db *DB) nodeAt(off uint64, level int) []byte {
	h := db.tree[off : off+32]
	if le.Uint64(h) == 0 && le.Uint64(h[8:]) == 0 && le.Uint64(h[16:]) == 0 && le.Uint64(h[24:]) == 0 {
		return db.empty[level][:]
	}
	return h
}

// parallel runs f over [0, n) split across GOMAXPROCS goroutines.
func parallel(n int, f func(lo, hi int)) {
	w := runtime.GOMAXPROCS(0)
	if n < 256 || w == 1 {
		f(0, n)
		return
	}
	var wg sync.WaitGroup
	step := (n + w - 1) / w
	for lo := 0; lo < n; lo += step {
		wg.Go(func() { f(lo, min(lo+step, n)) })
	}
	wg.Wait()
}

// parallelGroups splits sorted idx across GOMAXPROCS workers at boundaries
// where i>>span changes and calls f for each group of equal i>>span.
func parallelGroups(idx []uint64, span uint, f func([]uint64)) {
	run := func(part []uint64) {
		for len(part) > 0 {
			n := 1
			for n < len(part) && part[n]>>span == part[0]>>span {
				n++
			}
			f(part[:n])
			part = part[n:]
		}
	}
	w := runtime.GOMAXPROCS(0)
	if len(idx) < 256 || w == 1 {
		run(idx)
		return
	}
	var wg sync.WaitGroup
	step := (len(idx) + w - 1) / w
	for lo := 0; lo < len(idx); {
		hi := min(lo+step, len(idx))
		for hi < len(idx) && idx[hi]>>span == idx[hi-1]>>span {
			hi++
		}
		part := idx[lo:hi]
		wg.Go(func() { run(part) })
		lo = hi
	}
	wg.Wait()
}

// hashBand hashes the ancestors of children (sorted, at heap depth d, all under
// one node at depth top) from depth d-1 up to top. All parents share one tile.
func (db *DB) hashBand(children []uint64, d, top int) {
	var cur [128]uint64 // d-top <= 7, so at most 128 children
	n := copy(cur[:], children)
	var buf [64]byte
	for pd := d - 1; pd >= top; pd-- {
		t, c := tileTables[db.depth][pd], tileTables[db.depth][pd+1]
		// Siblings share a tile (32 bytes apart) unless they are tile roots.
		sib := uint64(32)
		if c.shift == 0 {
			sib = 4096
		}
		level := db.depth - pd - 1 // child level, 0 = leaf
		k := 0
		for _, i := range cur[:n] {
			p := i >> 1
			if k > 0 && cur[k-1] == p {
				continue
			}
			cur[k] = p
			k++
			l := c.base + (2*p>>c.shift)*4096 + (2*p&c.mask)*32
			copy(buf[:32], db.nodeAt(l, level))
			copy(buf[32:], db.nodeAt(l+sib, level))
			h := sha256.Sum256(buf[:])
			copy(db.tree[t.base+(p>>t.shift)*4096+(p&t.mask)*32:], h[:])
		}
		n = k
	}
}

// hashDirty hashes the dirty leaves, then their paths level by level (each node once),
// clears the dirty set and returns the root.
func (db *DB) hashDirty() [32]byte {
	slices.Sort(db.dirty)
	idx := make([]uint64, len(db.dirty))
	for i, l := range db.dirty {
		idx[i] = db.maxLeaves + uint64(l)
		db.dirtyBit[l/64] &^= 1 << (l % 64)
	}
	db.dirty = db.dirty[:0]
	leafT := tileTables[db.depth][db.depth]
	parallel(len(idx), func(lo, hi int) {
		for _, i := range idx[lo:hi] {
			off := (i - db.maxLeaves) * LeafSize
			h := sha256.Sum256(db.state[off : off+LeafSize])
			copy(db.tree[leafT.base+(i>>leafT.shift)*4096+(i&leafT.mask)*32:], h[:])
		}
	})
	// Climb one tile band at a time: every group of nodes below one tile root is
	// finished while that tile is in cache, instead of sweeping each level.
	for d := db.depth; d > 0; {
		top := d - 1 - int(tileTables[db.depth][d-1].shift)
		span := uint(d - top)
		parallelGroups(idx, span, func(group []uint64) { db.hashBand(group, d, top) })
		k := 0
		for _, i := range idx {
			if p := i >> span; k == 0 || idx[k-1] != p {
				idx[k] = p
				k++
			}
		}
		idx = idx[:k]
		d = top
	}
	return [32]byte(db.node(1, db.depth))
}

// Commit fills the new bytes of Touch entries, hashes the dirty leaves and their paths,
// appends the block record and advances the version. It may close the epoch.
func (db *DB) Commit() ([32]byte, uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.sealErr != nil {
		return db.root, db.version, db.sealErr
	}
	if db.version == ^uint64(0) {
		return db.root, db.version, errors.New("pagerdb: version limit reached")
	}
	c := db.cur
	db.fillNewBytes()
	db.touches = db.touches[:0]
	root := db.hashDirty()
	pos := db.reserve(blockRec)
	r := c.m[pos : pos+blockRec]
	le.PutUint32(r[4:], uint32(db.seq()-db.blockSeq))
	le.PutUint64(r[8:], db.version+1)
	copy(r[16:48], root[:])
	le.PutUint32(r[48:], crc32.Update(crc32.Checksum(c.m[db.blockStart:pos], castagnoli), castagnoli, r[4:48]))
	r[0] = 'B'
	db.resetUndoKeys()
	db.version++
	db.root = root
	db.blocks = append(db.blocks, block{ver: db.version, root: root, c: c, end: c.tail})
	db.rootIdx[root] = db.version
	db.blockStart, db.blockSeq = c.tail, db.seq()
	db.pruneIndex()
	if db.version-c.start >= db.o.EpochVersions || c.tail >= db.o.EpochBytes {
		if err := db.rotate(); err != nil {
			return root, db.version, err
		}
	}
	return root, db.version, nil
}

// fillNewBytes stores the block's final image for each logged range. Recovery
// replays complete blocks, so overlapping ranges all carry the same final bytes.
// Historical reads and sealing use undo images, not intermediate new bytes.
func (db *DB) fillNewBytes() {
	db.indexPending()
	parallel(int(db.entries.end-db.blockSeq), func(lo, hi int) {
		for i := lo; i < hi; i++ {
			m, off, n := db.rec(db.entries.at(db.blockSeq + int64(i)))
			copy(m[entryHdr+n:entryHdr+2*n], db.state[off:off+n])
		}
	})
}
