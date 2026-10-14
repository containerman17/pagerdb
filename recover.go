package pagerdb

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"golang.org/x/sys/unix"
)

// Mark is a position in the pending block's log.
type Mark struct {
	version  uint64
	seq, pos int64
	touches  int
}

// Mark returns the current log position for Undo.
func (db *DB) Mark() Mark {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.mark()
}

func (db *DB) mark() Mark { return Mark{db.version, db.seq(), db.cur.tail, len(db.touches)} }

// Undo reverts every write, Touch and allocation logged since m. m must be from the pending block.
func (db *DB) Undo(m Mark) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if m.version != db.version || m.seq < db.blockSeq || m.seq > db.seq() {
		panic("pagerdb: Undo with a mark from another block")
	}
	db.undo(m)
}

func (db *DB) undo(m Mark) {
	db.undoTo(m.pos, m.seq)
	db.touches = db.touches[:m.touches]
}

// undoTo restores the old bytes of every entry from seq on (newest first), removes them from
// the log and the index, and cuts the current chunk back to pos.
func (db *DB) undoTo(pos, seq int64) {
	for i := db.seq() - 1; i >= seq; i-- {
		e := db.entries[i-db.entryBase]
		old := e.c.m[e.pos+entryHdr : e.pos+entryHdr+int64(e.n)]
		copy(db.state[e.off:e.off+uint64(e.n)], old)
		db.markDirty(e.off, uint64(e.n))
		e.c.m[e.pos] = 0
		for p := db.pieceSeq() - 1; p >= e.piece; p-- {
			pc := db.pieces[p-db.pieceBase]
			if pc.prev >= db.pieceBase {
				db.heads[pc.leaf] = pc.prev
			} else {
				delete(db.heads, pc.leaf)
			}
		}
		db.pieces = db.pieces[:e.piece-db.pieceBase]
		db.entries = db.entries[:i-db.entryBase]
	}
	clear(db.cur.m[pos:db.cur.tail])
	db.cur.tail = pos
	clear(db.segCache)
	for r, m := range db.maps {
		if l, err := locate(db.headU64, db.regTab, db.maxRegions, r); err != nil || !slices.Equal(l.segs, db.loc(r).segs) || len(m) != int(l.size) {
			db.forget(r)
		}
	}
}

// scanned is one complete block found in a chunk.
type scanned struct {
	ver     uint64
	root    [32]byte
	entries []int64
	end     int64
}

// scan parses a chunk: checkpoint, complete blocks (count and crc checked), and the entries
// after the last complete block (the torn tail).
func scan(m []byte) (cpVer uint64, cpRoot [32]byte, blocks []scanned, tail []int64, ok bool) {
	if len(m) < cpRec || m[0] != 'C' {
		return 0, cpRoot, nil, nil, false
	}
	cpVer, cpRoot = le.Uint64(m[8:]), [32]byte(m[16:48])
	pos, start, ver := int64(cpRec), int64(cpRec), cpVer
	var cur []int64
loop:
	for pos < int64(len(m)) {
		switch m[pos] {
		case 'E':
			if pos+entryHdr > int64(len(m)) {
				break loop
			}
			sz := entryHdr + 2*int64(le.Uint32(m[pos+8:]))
			if pos+sz > int64(len(m)) {
				break loop
			}
			cur = append(cur, pos)
			pos += sz
		case 'B':
			if pos+blockRec > int64(len(m)) {
				break loop
			}
			r := m[pos : pos+blockRec]
			if int(le.Uint32(r[4:])) != len(cur) || le.Uint64(r[8:]) != ver+1 ||
				le.Uint32(r[48:]) != crc32.Checksum(m[start:pos], castagnoli) {
				break loop
			}
			ver++
			pos += blockRec
			blocks = append(blocks, scanned{ver: ver, root: [32]byte(r[16:48]), entries: cur, end: pos})
			cur, start = nil, pos
		default:
			break loop
		}
	}
	return cpVer, cpRoot, blocks, cur, true
}

func openChunk(path string, epoch uint64) (*chunk, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil || fi.Size() < cpRec {
		f.Close()
		return nil, err
	}
	m, err := unix.Mmap(int(f.Fd()), 0, int(fi.Size()), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &chunk{epoch: epoch, f: f, m: m}, nil
}

// open recovers the store: the last chunk with a checkpoint is replayed (complete blocks
// forward with new bytes, the torn tail backwards with old bytes), the leaves it touched are
// rehashed and the root must match its last block record. Older unsealed chunks are loaded
// into the window and queued for sealing.
func (db *DB) open() error {
	db.heads = map[uint32]int64{}
	db.rootIdx = map[[32]byte]uint64{}
	db.pins = map[uint64]int{}
	db.sealCond.L = &db.mu
	db.sealDone = make(chan struct{})

	des, err := os.ReadDir(filepath.Join(db.dir, "log"))
	if err != nil {
		return err
	}
	var epochs []uint64
	for _, de := range des {
		if e, err := strconv.ParseUint(de.Name(), 10, 64); err == nil {
			epochs = append(epochs, e)
		}
	}
	slices.Sort(epochs)
	// A chunk without a complete checkpoint is a crash during rotation: drop it.
	var last *chunk
	for len(epochs) > 0 && last == nil {
		e := epochs[len(epochs)-1]
		c, err := openChunk(db.chunkPath(e), e)
		if err != nil {
			return err
		}
		if c != nil && c.m[0] == 'C' {
			last = c
			break
		}
		if c != nil {
			c.close()
		}
		if err := os.Remove(db.chunkPath(e)); err != nil {
			return err
		}
		epochs = epochs[:len(epochs)-1]
	}
	if last == nil {
		if db.o.Store.Latest() != 0 {
			return fmt.Errorf("pagerdb: %s: sealed epochs but no live log", db.dir)
		}
		if err := db.mapFiles(true); err != nil {
			return err
		}
		db.root = db.hashDirty()
		c, err := db.newChunk(1, 0, db.root)
		if err != nil {
			return err
		}
		db.cur, db.chunks = c, []*chunk{c}
		db.blocks = []block{{ver: 0, root: db.root, c: c, end: cpRec}}
		db.rootIdx[db.root] = 0
		db.blockStart = cpRec
		go db.sealer()
		return nil
	}
	if err := db.mapFiles(false); err != nil {
		last.close()
		return err
	}
	if err := db.loadSealed(); err != nil {
		last.close()
		return err
	}
	latest := db.o.Store.Latest()
	var queue []*chunk
	for _, e := range epochs[:len(epochs)-1] {
		if e <= latest {
			// Sealed before the crash, only the delete was missing.
			if err := os.Remove(db.chunkPath(e)); err != nil {
				return err
			}
			continue
		}
		c, err := openChunk(db.chunkPath(e), e)
		if err != nil || c == nil {
			last.close()
			return fmt.Errorf("pagerdb: chunk %d: %v", e, err)
		}
		cpVer, cpRoot, bl, _, ok := scan(c.m)
		if !ok {
			c.close()
			last.close()
			return fmt.Errorf("pagerdb: chunk %d: no checkpoint", e)
		}
		c.start = cpVer
		c.tail = int64(len(c.m))
		db.chunks = append(db.chunks, c)
		db.addBlocks(c, cpVer, cpRoot, bl)
		queue = append(queue, c)
	}
	db.chunks = append(db.chunks, last)
	db.cur = last
	if err := db.replay(last); err != nil {
		return err
	}
	go db.sealer()
	db.mu.Lock()
	for _, c := range queue {
		db.enqueueSeal(c)
	}
	db.mu.Unlock()
	return nil
}

func (db *DB) addBlocks(c *chunk, cpVer uint64, cpRoot [32]byte, bl []scanned) {
	db.blocks = append(db.blocks, block{ver: cpVer, root: cpRoot, c: c, end: cpRec})
	db.rootIdx[cpRoot] = cpVer
	for _, b := range bl {
		for _, pos := range b.entries {
			e := c.m[pos:]
			db.index(c, pos, le.Uint64(e[16:]), le.Uint64(e[24:]), uint64(le.Uint32(e[8:])))
		}
		db.blocks = append(db.blocks, block{ver: b.ver, root: b.root, c: c, end: b.end})
		db.rootIdx[b.root] = b.ver
	}
}

func (db *DB) replay(c *chunk) error {
	cpVer, cpRoot, bl, tail, _ := scan(c.m)
	c.start = cpVer
	need := uint64(0)
	for _, b := range bl {
		for _, pos := range b.entries {
			need = max(need, le.Uint64(c.m[pos+24:])+uint64(le.Uint32(c.m[pos+8:])))
		}
	}
	for _, pos := range tail {
		need = max(need, le.Uint64(c.m[pos+24:])+uint64(le.Uint32(c.m[pos+8:])))
	}
	if err := db.growState((need + SegSize - 1) / SegSize); err != nil {
		return err
	}
	apply := func(pos int64, newBytes bool) {
		n, fo := uint64(le.Uint32(c.m[pos+8:])), le.Uint64(c.m[pos+24:])
		src := c.m[pos+entryHdr:]
		if newBytes {
			src = src[n:]
		}
		copy(db.state[fo:fo+n], src[:n])
		db.markDirty(fo, n)
	}
	for _, b := range bl {
		for _, pos := range b.entries {
			apply(pos, true)
		}
	}
	for i := len(tail) - 1; i >= 0; i-- {
		apply(tail[i], false)
	}
	end := int64(cpRec)
	db.version, db.root = cpVer, cpRoot
	if len(bl) > 0 {
		end = bl[len(bl)-1].end
		db.version, db.root = bl[len(bl)-1].ver, bl[len(bl)-1].root
	}
	clear(c.m[end:])
	c.tail = end
	db.addBlocks(c, cpVer, cpRoot, bl)
	db.blockStart, db.blockSeq = end, db.seq()
	if err := db.growState(db.hdr(hNextSeg)); err != nil {
		return err
	}
	if root := db.hashDirty(); root != db.root {
		return fmt.Errorf("pagerdb: recovery: root %x does not match block %d root %x", root, db.version, db.root)
	}
	return nil
}

// Rewind moves the head back to version v, which must be in the current epoch: the old bytes
// of every newer entry are restored newest first, the leaves rehashed and the log cut at v.
func (db *DB) Rewind(v uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if v > db.version {
		return ErrVersion
	}
	// shortcut: only the current epoch can be rewound; crossing into a sealed epoch needs a
	// delete in EpochStore, add it when the settled version can lag behind an epoch boundary.
	if v < db.cur.start {
		return fmt.Errorf("%w: rewind to %d crosses the epoch that starts at %d", ErrWindow, v, db.cur.start)
	}
	for pv := range db.pins {
		if pv > v {
			return fmt.Errorf("pagerdb: rewind to %d with an open view at %d", v, pv)
		}
	}
	i := slices.IndexFunc(db.blocks, func(b block) bool { return b.c == db.cur && b.ver == v })
	b := db.blocks[i]
	seq := db.seq()
	for seq > db.entryBase && db.entries[seq-1-db.entryBase].c == db.cur && db.entries[seq-1-db.entryBase].pos >= b.end {
		seq--
	}
	db.undoTo(b.end, seq)
	db.touches = db.touches[:0]
	if root := db.hashDirty(); root != b.root {
		return fmt.Errorf("pagerdb: rewind: root %x does not match version %d root %x", root, v, b.root)
	}
	db.version, db.root = v, b.root
	db.blocks = db.blocks[:i+1]
	clear(db.rootIdx)
	for _, b := range db.blocks {
		db.rootIdx[b.root] = b.ver
	}
	db.blockStart, db.blockSeq = b.end, seq
	return nil
}
