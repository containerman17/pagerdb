package pagerdb

import (
	"fmt"
	"os"
	"slices"
	"sort"
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
	if v >= db.chunks[0].start && v < db.windowStart && (len(db.sealed) == 0 || v < db.sealed[0].start || v > db.sealed[len(db.sealed)-1].end) {
		return nil, ErrWindow // pruned from the index and not sealed yet
	}
	if v >= db.windowStart && v >= db.chunks[0].start && (len(db.sealed) == 0 || v < db.sealed[0].start || v >= db.sealed[len(db.sealed)-1].end) {
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
	if vw.closed {
		db.mu.Unlock()
		return
	}
	vw.closed = true
	vw.closeMaps()
	if db.pins[vw.v]--; db.pins[vw.v] == 0 {
		delete(db.pins, vw.v)
	}
	dropped := db.dropChunks()
	db.mu.Unlock()
	db.closeChunks(dropped)
}

// loc resolves r at the view's version. held means the caller holds db.mu
// and the view is a window view.
func (vw *View) loc(r Region, held bool) (loc, error) {
	var err error
	rd := func(off uint64) uint64 {
		var b [8]byte
		if held {
			vw.db.readWindow(vw.v, off, b[:])
		} else if e := vw.readFile(off, b[:]); e != nil {
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
	if vw.window {
		// One lock acquisition covers the region lookup and the data.
		vw.db.mu.Lock()
		defer vw.db.mu.Unlock()
		if vw.closed {
			return fmt.Errorf("pagerdb: view closed")
		}
		l, err := vw.loc(r, true)
		if err != nil {
			return err
		}
		if off < 0 || n < 0 || uint64(off+n) > l.size {
			return fmt.Errorf("pagerdb: range [%d,%d) outside region of %d bytes", off, off+n, l.size)
		}
		l.each(uint64(off), uint64(n), func(fo, pos, k uint64) { vw.db.readWindow(vw.v, fo, buf[pos:pos+k]) })
		return nil
	}
	l, err := vw.loc(r, false)
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
	l, err := vw.loc(r, false)
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

// readWindow reads state bytes at version v inside the window, one leaf at a
// time. Caller holds db.mu.
func (db *DB) readWindow(v, fo uint64, b []byte) {
	for pos := uint64(0); pos < uint64(len(b)); {
		o := fo + pos
		n := min(uint64(len(b))-pos, LeafSize-o%LeafSize)
		db.readWindowRange(v, o/LeafSize, o%LeafSize, b[pos:pos+n])
		pos += n
	}
}

// cutIndex removes the oldest k entries and their pieces. Stale heads stay
// until dropChunks; readers treat heads below pieceBase as absent.
func (db *DB) cutIndex(k int) {
	if k == 0 {
		return
	}
	first := db.entryBase + int64(k)
	pb := db.pieceBase + int64(sort.Search(len(db.pieces), func(j int) bool { return db.pieceEntry(db.pieces[j]) >= first }))
	db.pieces = db.pieces[pb-db.pieceBase:]
	db.pieceBase = pb
	db.entries = db.entries[k:]
	db.entryBase = first
}

// pruneIndex frees index entries of versions no rewind, open view or window
// sealing needs, so memory follows RetainVersions instead of whole epochs.
// Versions it passes stay readable once their epoch is sealed. Caller holds db.mu.
func (db *DB) pruneIndex() {
	if db.version <= db.o.RetainVersions {
		return
	}
	p := db.version - db.o.RetainVersions
	for v := range db.pins {
		p = min(p, v)
	}
	// The sealer may rebuild an unsealed chunk's end state from the window.
	for i, c := range db.chunks[:len(db.chunks)-1] {
		if !c.sealed {
			p = min(p, db.chunks[i+1].start)
			break
		}
	}
	if p <= db.windowStart {
		return
	}
	db.cutIndex(sort.Search(len(db.entries), func(j int) bool { return db.entries[j].ver > p }))
	db.windowStart = p
}

// dropChunks removes sealed chunks from the window once no view needs them. Caller holds db.mu.
func (db *DB) dropChunks() (dropped []*chunk) {
	for len(db.chunks) > 1 && db.chunks[0].sealed && db.chunks[1].epoch <= db.durableEpoch {
		c, next := db.chunks[0], db.chunks[1].start
		if db.version <= db.o.RetainVersions || next > db.version-db.o.RetainVersions {
			return dropped
		}
		for pv := range db.pins {
			if pv < next {
				return dropped
			}
		}
		k := 0
		for k < len(db.entries) && db.entries[k].epoch() == uint32(c.epoch)&(1<<24-1) {
			k++
		}
		db.cutIndex(k)
		for l, p := range db.heads {
			if p < db.pieceBase {
				delete(db.heads, l)
			}
		}
		db.windowStart = max(db.windowStart, next)
		db.blocks = slices.DeleteFunc(db.blocks, func(b block) bool {
			if b.c == c && db.rootIdx[b.root] == b.ver && b.ver != next {
				delete(db.rootIdx, b.root)
			}
			return b.c == c
		})
		db.chunks = db.chunks[1:]
		dropped = append(dropped, c)
	}
	return dropped
}

// closeChunks unmaps and deletes chunks that dropChunks unlinked. Callers run
// it after releasing db.mu, so the owner does not wait for munmap and unlink.
func (db *DB) closeChunks(dropped []*chunk) {
	for _, c := range dropped {
		c.close()
		os.Remove(db.chunkPath(c.epoch))
	}
}
