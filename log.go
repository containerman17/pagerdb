package pagerdb

import (
	"fmt"
	"hash/crc32"
	bits64 "math/bits"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Live log records. The tag byte is written last, so a record with a tag is complete.
//
//	entry:      'E' _[3] region u32 | n u32 undo crc32c u32 | version u64 | state offset u64 | old [n] | new [n]
//	block:      'B' _[3] entries u32 | version u64 | root [32] | crc32c u32 _[4]   (after its entries)
//	checkpoint: 'C' _[7] | version u64 | root [32] | crc32c u32 _[4]              (first record of a chunk)
const (
	entryHdr  = 32
	blockRec  = 56
	cpRec     = 56
	chunkInit = 64 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// chunk is one live log file, one per epoch.
type chunk struct {
	epoch  uint64
	start  uint64 // version of the checkpoint that opens it
	f      *os.File
	m      []byte
	tail   int64
	sealed bool
}

type entry struct {
	ver   uint64
	off   uint64 // state file offset
	n     uint32
	c     *chunk
	pos   int64 // record position in c
	piece int64 // seq of its first piece
}

// piece links an entry into the per-leaf chain (newest first) the views walk.
type piece struct {
	entry, prev int64
	leaf        uint32
}

type block struct {
	ver  uint64
	root [32]byte
	c    *chunk
	end  int64 // log position after the record
}

type logState struct {
	cur        *chunk
	chunks     []*chunk // window, oldest first; cur is last
	blockStart int64    // log position of the pending block's first entry
	blockSeq   int64    // entry seq of the pending block's first entry
	touches    []int64  // Touch entries of the pending block, new bytes filled at Commit

	entries   []entry
	entryBase int64
	pieces    []piece
	pieceBase int64
	heads     map[uint32]int64 // leaf -> newest piece seq
	blocks    []block          // window versions, checkpoints included
	rootIdx   map[[32]byte]uint64

	// Sync mode stages written bytes here until the pending block's log is
	// durable, so a shared state page never holds a change without its undo
	// record on disk. Entries from seq appliedSeq on are staged while the map
	// is non-empty.
	stage      map[uint32]*stageLeaf
	spare      []*stageLeaf
	appliedSeq int64
	// logHigh is the end of log bytes that an undo cleared since the last
	// barrier. The next barrier covers them, so undone entries that writeback
	// already stored cannot reappear as a torn tail after power loss.
	logHigh int64
}

// stageLeaf overlays one leaf: data where the cov bit is set, state elsewhere.
type stageLeaf struct {
	data [LeafSize]byte
	cov  [LeafSize / 64]uint64
}

// overlay copies staged bytes of [at, at+len(out)) into out.
func (s *stageLeaf) overlay(out []byte, at uint64) {
	for i := range out {
		b := at + uint64(i)
		if s.cov[b/64]>>(b%64)&1 != 0 {
			out[i] = s.data[b]
		}
	}
}

// readHead copies head bytes at state offset fo, including staged bytes.
func (db *DB) readHead(fo uint64, out []byte) {
	copy(out, db.state[fo:])
	if len(db.stage) == 0 {
		return
	}
	for pos := uint64(0); pos < uint64(len(out)); {
		o := fo + pos
		k := min(uint64(len(out))-pos, LeafSize-o%LeafSize)
		if s := db.stage[uint32(o/LeafSize)]; s != nil {
			s.overlay(out[pos:pos+k], o%LeafSize)
		}
		pos += k
	}
}

// stageBytes records b at state offset fo without touching the state mapping.
func (db *DB) stageBytes(fo uint64, b []byte) {
	if db.stage == nil {
		db.stage = map[uint32]*stageLeaf{}
	}
	for pos := uint64(0); pos < uint64(len(b)); {
		o := fo + pos
		at := o % LeafSize
		k := min(uint64(len(b))-pos, LeafSize-at)
		s := db.stage[uint32(o/LeafSize)]
		if s == nil {
			if n := len(db.spare); n > 0 {
				s, db.spare = db.spare[n-1], db.spare[:n-1]
				clear(s.cov[:])
			} else {
				s = new(stageLeaf)
			}
			db.stage[uint32(o/LeafSize)] = s
		}
		copy(s.data[at:at+k], b[pos:pos+k])
		for i := at; i < at+k; i++ {
			s.cov[i/64] |= 1 << (i % 64)
		}
		pos += k
	}
}

// applyStage writes staged bytes into the state mapping and empties the stage.
func (db *DB) applyStage() {
	for leaf, s := range db.stage {
		base := uint64(leaf) * LeafSize
		for w, bits := range s.cov {
			at := uint64(w) * 64
			if bits == ^uint64(0) {
				copy(db.state[base+at:base+at+64], s.data[at:at+64])
				continue
			}
			for ; bits != 0; bits &= bits - 1 {
				i := at + uint64(bits64.TrailingZeros64(bits))
				db.state[base+i] = s.data[i]
			}
		}
		db.spare = append(db.spare, s)
	}
	clear(db.stage)
}

// restage rebuilds the stage from the staged entries that remain after an undo.
func (db *DB) restage() {
	for _, s := range db.stage {
		db.spare = append(db.spare, s)
	}
	clear(db.stage)
	for i := db.appliedSeq; i < db.seq(); i++ {
		e := db.entries[i-db.entryBase]
		n := int64(e.n)
		db.stageBytes(e.off, e.c.m[e.pos+entryHdr+n:e.pos+entryHdr+2*n])
	}
}

// syncPending makes the pending block's log durable, then applies staged bytes.
func (db *DB) syncPending() error {
	c := db.cur
	from := db.blockStart / int64(os.Getpagesize()) * int64(os.Getpagesize())
	if err := unix.Msync(c.m[from:max(c.tail, db.logHigh)], unix.MS_SYNC); err != nil {
		return err
	}
	if err := c.f.Sync(); err != nil {
		return err
	}
	db.logHigh = c.tail
	db.applyStage()
	return nil
}

func (db *DB) seq() int64      { return db.entryBase + int64(len(db.entries)) }
func (db *DB) pieceSeq() int64 { return db.pieceBase + int64(len(db.pieces)) }

func (db *DB) chunkPath(epoch uint64) string {
	return filepath.Join(db.dir, "log", fmt.Sprintf("%012d", epoch))
}

func (c *chunk) close() {
	if c.m != nil {
		unix.Munmap(c.m)
		c.m = nil
	}
	c.f.Close()
}

// newChunk creates the log file of an epoch, opened by a checkpoint mark.
func (db *DB) newChunk(epoch, ver uint64, root [32]byte) (*chunk, error) {
	f, err := os.OpenFile(db.chunkPath(epoch), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	c := &chunk{epoch: epoch, start: ver, f: f, tail: cpRec}
	if err = f.Truncate(chunkInit); err == nil {
		c.m, err = unix.Mmap(int(f.Fd()), 0, chunkInit, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	le.PutUint64(c.m[8:], ver)
	copy(c.m[16:48], root[:])
	le.PutUint32(c.m[48:], crc32.Checksum(c.m[8:48], castagnoli))
	c.m[0] = 'C'
	if db.o.Sync {
		if err = unix.Msync(c.m[:cpRec], unix.MS_SYNC); err == nil {
			err = c.f.Sync()
		}
		if err == nil {
			err = syncDir(filepath.Dir(c.f.Name()))
		}
	}
	return c, err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// reserve makes room for sz bytes at the tail of the current chunk.
func (db *DB) reserve(sz int64) int64 {
	c := db.cur
	if c.tail+sz > int64(len(c.m)) {
		n := max(2*int64(len(c.m)), c.tail+sz)
		if err := c.f.Truncate(n); err != nil {
			panic(fmt.Sprintf("pagerdb: grow log: %v", err))
		}
		unix.Munmap(c.m)
		m, err := unix.Mmap(int(c.f.Fd()), 0, int(n), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if err != nil {
			panic(fmt.Sprintf("pagerdb: map log: %v", err))
		}
		c.m = m
	}
	pos := c.tail
	c.tail += sz
	return pos
}

// logEntry appends an entry for n bytes at state offset fo. With b != nil it also stores b
// (after the entry is complete, so every state change has its old bytes in the log).
func (db *DB) logEntry(r Region, fo, n uint64, b []byte) int64 {
	pos := db.reserve(entryHdr + 2*int64(n))
	e := db.cur.m[pos : pos+entryHdr+2*int64(n)]
	le.PutUint32(e[4:], uint32(r))
	le.PutUint32(e[8:], uint32(n))
	le.PutUint64(e[16:], db.version+1)
	le.PutUint64(e[24:], fo)
	db.readHead(fo, e[entryHdr:entryHdr+n])
	if b != nil {
		copy(e[entryHdr+n:], b)
	}
	le.PutUint32(e[12:], entryChecksum(e, n))
	e[0] = 'E'
	// In Sync mode the undo image must reach disk before any shared state byte
	// can change, so new bytes wait in the stage for the block's log barrier.
	if b != nil {
		if db.o.Sync {
			if len(db.stage) == 0 {
				db.appliedSeq = db.seq()
			}
			db.stageBytes(fo, e[entryHdr+n:])
		} else {
			copy(db.state[fo:fo+n], e[entryHdr+n:])
		}
	}
	db.markDirty(fo, n)
	db.index(db.cur, pos, db.version+1, fo, n)
	return pos
}

func (db *DB) put(r Region, fo uint64, b []byte) { db.logEntry(r, fo, uint64(len(b)), b) }

func (db *DB) index(c *chunk, pos int64, ver, fo, n uint64) {
	seq := db.seq()
	db.entries = append(db.entries, entry{ver: ver, off: fo, n: uint32(n), c: c, pos: pos, piece: db.pieceSeq()})
	for l := fo / LeafSize; l <= (fo+n-1)/LeafSize; l++ {
		prev, ok := db.heads[uint32(l)]
		if !ok {
			prev = -1
		}
		db.heads[uint32(l)] = db.pieceSeq()
		db.pieces = append(db.pieces, piece{entry: seq, prev: prev, leaf: uint32(l)})
	}
}

// Write logs the old and new bytes, stores b at off and marks the leaves dirty.
func (db *DB) Write(r Region, off int, b []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.loc(r).each(uint64(off), uint64(len(b)), func(fo, pos, k uint64) { db.put(r, fo, b[pos:pos+k]) })
}

// Touch announces a direct write through Map: the old bytes are logged now, the new bytes
// are read at Commit. In Sync mode it makes the log durable (one barrier) and
// applies staged writes to the state mapping before returning.
func (db *DB) Touch(r Region, off, n int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.loc(r).each(uint64(off), uint64(n), func(fo, pos, k uint64) {
		db.touches = append(db.touches, db.logEntry(r, fo, k, nil))
	})
	if db.o.Sync {
		if err := db.syncPending(); err != nil {
			panic(fmt.Sprintf("pagerdb: durable undo: %v", err))
		}
	}
}

// rotate closes the current epoch and opens the next with a checkpoint mark.
func (db *DB) rotate() error {
	// Bound future history that the sealer must undo to reconstruct its end state.
	// Recovery can enqueue an existing backlog; new rotations wait for it to drain.
	if db.sealPending >= 2 && db.sealErr == nil && !db.sealStop {
		db.sealWaits++
	}
	for db.sealPending >= 2 && db.sealErr == nil && !db.sealStop {
		db.sealCond.Wait()
	}
	if db.sealErr != nil {
		return db.sealErr
	}
	if db.sealStop {
		return fmt.Errorf("pagerdb: sealer stopped")
	}
	old := db.cur
	if err := old.f.Truncate(old.tail); err != nil {
		return err
	}
	if db.o.Sync {
		if err := unix.Msync(old.m[:old.tail], unix.MS_SYNC); err != nil {
			return err
		}
	}
	if err := db.syncState(); err != nil {
		return err
	}
	c, err := db.newChunk(old.epoch+1, db.version, db.root)
	if err != nil {
		return err
	}
	db.cur, db.logHigh = c, 0
	db.chunks = append(db.chunks, c)
	db.blocks = append(db.blocks, block{ver: db.version, root: db.root, c: c, end: cpRec})
	db.blockStart, db.blockSeq = c.tail, db.seq()
	db.enqueueSeal(old)
	return nil
}

func (db *DB) syncState() error {
	if err := unix.Msync(db.state[:db.hdr(hNextSeg)*SegSize], unix.MS_SYNC); err != nil {
		return err
	}
	if err := unix.Msync(db.tree, unix.MS_SYNC); err != nil {
		return err
	}
	if err := db.stateF.Sync(); err != nil {
		return err
	}
	return db.treeF.Sync()
}

// entryChecksum protects the immutable undo image. New bytes are protected by
// the block checksum because Touch fills them only at Commit.
func entryChecksum(e []byte, n uint64) uint32 {
	crc := crc32.Checksum(e[4:12], castagnoli)
	crc = crc32.Update(crc, castagnoli, e[16:entryHdr])
	return crc32.Update(crc, castagnoli, e[entryHdr:entryHdr+n])
}
