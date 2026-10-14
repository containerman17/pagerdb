package pagerdb

import (
	"fmt"
	"os"
	"slices"
)

type viewState struct {
	pins map[uint64]int // version -> open window views
}

// View is a read-only image of the state at one committed version.
type View struct {
	db       *DB
	v        uint64
	window   bool // served from head plus the live log, else from sealed epochs
	closed   bool
	mappings []*viewMapping
}

// View opens the state at version v.
func (db *DB) View(v uint64) (*View, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.view(v)
}

func (db *DB) view(v uint64) (*View, error) {
	if v > db.version {
		return nil, ErrVersion
	}
	if v < db.chunks[0].start && (len(db.sealed) == 0 || v < db.sealed[0].start || v > db.sealed[len(db.sealed)-1].end) {
		return nil, ErrVersion
	}
	if v >= db.chunks[0].start && (len(db.sealed) == 0 || v < db.sealed[0].start || v >= db.sealed[len(db.sealed)-1].end) {
		db.pins[v]++
		return &View{db: db, v: v, window: true}, nil
	}
	db.pins[v]++
	return &View{db: db, v: v}, nil
}

// ViewRoot opens the state whose root is root (newest version with that root).
func (db *DB) ViewRoot(root [32]byte) (*View, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if v, ok := db.rootIdx[root]; ok {
		return db.view(v)
	}
	for i := len(db.sealed) - 1; i >= 0; i-- {
		v, ok, err := db.sealed[i].findRoot(root)
		if err != nil {
			return nil, err
		}
		if ok {
			return db.view(v)
		}
	}
	return nil, ErrVersion
}

// Version is the version the view reads.
func (vw *View) Version() uint64 { return vw.v }

// Close releases the view's pin on the live log window.
func (vw *View) Close() {
	db := vw.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if vw.closed {
		return
	}
	vw.closed = true
	vw.closeMaps()
	if db.pins[vw.v]--; db.pins[vw.v] == 0 {
		delete(db.pins, vw.v)
	}
	db.dropChunks()
}

func (vw *View) loc(r Region) (loc, error) {
	var err error
	rd := func(off uint64) uint64 {
		var b [8]byte
		if e := vw.readFile(off, b[:]); e != nil {
			err = e
		}
		return le.Uint64(b[:])
	}
	l, lerr := locate(rd, vw.db.regTab, vw.db.maxRegions, r)
	if err != nil {
		return loc{}, err
	}
	return l, lerr
}

// Read copies n bytes at off of region r, as of the view's version, into buf.
func (vw *View) Read(r Region, off, n int, buf []byte) error {
	l, err := vw.loc(r)
	if err != nil {
		return err
	}
	if off < 0 || n < 0 || uint64(off+n) > l.size {
		return fmt.Errorf("pagerdb: range [%d,%d) outside region of %d bytes", off, off+n, l.size)
	}
	l.each(uint64(off), uint64(n), func(fo, pos, k uint64) {
		if e := vw.readFile(fo, buf[pos:pos+k]); e != nil {
			err = e
		}
	})
	return err
}

// Copy returns the whole region as of the view's version.
func (vw *View) Copy(r Region) ([]byte, error) {
	l, err := vw.loc(r)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, l.size)
	l.each(0, l.size, func(fo, pos, k uint64) {
		if e := vw.readFile(fo, buf[pos:pos+k]); e != nil {
			err = e
		}
	})
	return buf, err
}

func (vw *View) readFile(fo uint64, b []byte) error {
	vw.db.mu.Lock()
	if vw.closed {
		vw.db.mu.Unlock()
		return fmt.Errorf("pagerdb: view closed")
	}
	if vw.window {
		defer vw.db.mu.Unlock()
		vw.db.readWindow(vw.v, fo, b)
		return nil
	}
	vw.db.mu.Unlock()
	for pos := uint64(0); pos < uint64(len(b)); {
		o := fo + pos
		n := min(uint64(len(b))-pos, LeafSize-o%LeafSize)
		if err := vw.db.sealedRange(o/LeafSize, vw.v, o%LeafSize, b[pos:pos+n]); err != nil {
			return err
		}
		pos += n
	}
	return nil
}

// readWindow reads state bytes at version v (inside the window): head bytes, then the old bytes
// of every overlapping entry newer than v applied newest first, so the oldest one wins.
// Caller holds db.mu.
func (db *DB) readWindow(v, fo uint64, b []byte) {
	n := uint64(len(b))
	if n == 0 {
		return
	}
	covered := make([]bool, len(b))
	var seqs []int64
	for l := fo / LeafSize; l <= (fo+n-1)/LeafSize; l++ {
		p, ok := db.heads[uint32(l)]
		for ok && p >= db.pieceBase {
			pc := db.pieces[p-db.pieceBase]
			if db.entries[pc.entry-db.entryBase].ver <= v {
				break
			}
			seqs = append(seqs, pc.entry)
			p = pc.prev
		}
	}
	if fo/LeafSize != (fo+n-1)/LeafSize {
		slices.Sort(seqs)
		seqs = slices.Compact(seqs)
		slices.Reverse(seqs)
	}
	for _, s := range seqs {
		e := db.entries[s-db.entryBase]
		lo, hi := max(e.off, fo), min(e.off+uint64(e.n), fo+n)
		if lo < hi {
			copy(b[lo-fo:hi-fo], e.c.m[e.pos+entryHdr+int64(lo-e.off):])
			for i := lo - fo; i < hi-fo; i++ {
				covered[i] = true
			}
		}
	}
	for i := 0; i < len(b); {
		if covered[i] {
			i++
			continue
		}
		start := i
		for i < len(b) && !covered[i] {
			i++
		}
		copy(b[start:i], db.state[fo+uint64(start):fo+uint64(i)])
	}
}

// dropChunks removes sealed chunks from the window once no view needs them. Caller holds db.mu.
func (db *DB) dropChunks() {
	for len(db.chunks) > 1 && db.chunks[0].sealed {
		c, next := db.chunks[0], db.chunks[1].start
		if db.version <= db.o.RetainVersions || next > db.version-db.o.RetainVersions {
			return
		}
		for pv := range db.pins {
			if pv < next {
				return
			}
		}
		k := 0
		for k < len(db.entries) && db.entries[k].c == c {
			k++
		}
		if k > 0 {
			pb := db.pieceSeq()
			if k < len(db.entries) {
				pb = db.entries[k].piece
			}
			db.pieces = db.pieces[pb-db.pieceBase:]
			db.pieceBase = pb
			clear(db.entries[:k])
			db.entries = db.entries[k:]
			db.entryBase += int64(k)
			for l, p := range db.heads {
				if p < db.pieceBase {
					delete(db.heads, l)
				}
			}
		}
		db.blocks = slices.DeleteFunc(db.blocks, func(b block) bool {
			if b.c == c && db.rootIdx[b.root] == b.ver && b.ver != next {
				delete(db.rootIdx, b.root)
			}
			return b.c == c
		})
		db.chunks = db.chunks[1:]
		c.close()
		os.Remove(db.chunkPath(c.epoch))
	}
}
