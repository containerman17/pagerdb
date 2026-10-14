package pagerdb

import (
	"crypto/sha256"
	"hash/crc32"
	"os"
	"runtime"
	"slices"
	"sync"

	"golang.org/x/sys/unix"
)

func sha256Sum(b []byte) [32]byte { return sha256.Sum256(b) }

func hashPair(a, b []byte) [32]byte {
	var buf [64]byte
	copy(buf[:32], a)
	copy(buf[32:], b)
	return sha256.Sum256(buf[:])
}

func (db *DB) markDirty(off, n uint64) {
	for l := off / LeafSize; l <= (off+n-1)/LeafSize; l++ {
		if db.dirtyBit[l/64]&(1<<(l%64)) == 0 {
			db.dirtyBit[l/64] |= 1 << (l % 64)
			db.dirty = append(db.dirty, uint32(l))
		}
	}
}

// node returns tree node i at level (0 = leaf). An all-zero slot is an untouched empty subtree.
func (db *DB) node(i uint64, level int) []byte {
	h := db.tree[i*32 : i*32+32]
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
	parallel(len(idx), func(lo, hi int) {
		for _, i := range idx[lo:hi] {
			off := (i - db.maxLeaves) * LeafSize
			h := sha256.Sum256(db.state[off : off+LeafSize])
			copy(db.tree[i*32:], h[:])
		}
	})
	for level := 1; level <= db.depth; level++ {
		k := 0
		for _, i := range idx {
			if p := i >> 1; k == 0 || idx[k-1] != p {
				idx[k] = p
				k++
			}
		}
		idx = idx[:k]
		parallel(len(idx), func(lo, hi int) {
			var buf [64]byte
			for _, i := range idx[lo:hi] {
				copy(buf[:32], db.node(2*i, level-1))
				copy(buf[32:], db.node(2*i+1, level-1))
				h := sha256.Sum256(buf[:])
				copy(db.tree[i*32:], h[:])
			}
		})
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
	c := db.cur
	for _, pos := range db.touches {
		n, fo := int64(le.Uint32(c.m[pos+8:])), le.Uint64(c.m[pos+24:])
		copy(c.m[pos+entryHdr+n:pos+entryHdr+2*n], db.state[fo:])
	}
	db.touches = db.touches[:0]
	root := db.hashDirty()
	pos := db.reserve(blockRec)
	r := c.m[pos : pos+blockRec]
	le.PutUint32(r[4:], uint32(db.seq()-db.blockSeq))
	le.PutUint64(r[8:], db.version+1)
	copy(r[16:48], root[:])
	le.PutUint32(r[48:], crc32.Checksum(c.m[db.blockStart:pos], castagnoli))
	r[0] = 'B'
	if db.o.Sync {
		from := db.blockStart / int64(os.Getpagesize()) * int64(os.Getpagesize())
		if err := unix.Msync(c.m[from:c.tail], unix.MS_SYNC); err != nil {
			return db.root, db.version, err
		}
	}
	db.version++
	db.root = root
	db.blocks = append(db.blocks, block{ver: db.version, root: root, c: c, end: c.tail})
	db.rootIdx[root] = db.version
	db.blockStart, db.blockSeq = c.tail, db.seq()
	if db.version-c.start >= db.o.EpochVersions || c.tail >= db.o.EpochBytes {
		if err := db.rotate(); err != nil {
			return root, db.version, err
		}
	}
	return root, db.version, nil
}
