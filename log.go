package pagerdb

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Live log records. The tag byte is written last, so a record with a tag is complete.
//
//	entry:      'E' _[3] region u32 | n u32 _[4] | version u64 | state offset u64 | old [n] | new [n]
//	block:      'B' _[3] entries u32 | version u64 | root [32] | crc32c u32 _[4]   (after its entries)
//	checkpoint: 'C' _[7] | version u64 | root [32]                                  (first record of a chunk)
const (
	entryHdr  = 32
	blockRec  = 56
	cpRec     = 48
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
	c.m[0] = 'C'
	if db.o.Sync {
		if err = unix.Msync(c.m[:cpRec], unix.MS_SYNC); err == nil {
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
	copy(e[entryHdr:], db.state[fo:fo+n])
	if b != nil {
		copy(e[entryHdr+n:], b)
	}
	e[0] = 'E'
	if b != nil {
		copy(db.state[fo:fo+n], e[entryHdr+n:])
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
// are read at Commit.
func (db *DB) Touch(r Region, off, n int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.loc(r).each(uint64(off), uint64(n), func(fo, pos, k uint64) {
		db.touches = append(db.touches, db.logEntry(r, fo, k, nil))
	})
}

// rotate closes the current epoch and opens the next with a checkpoint mark.
func (db *DB) rotate() error {
	old := db.cur
	if err := old.f.Truncate(old.tail); err != nil {
		return err
	}
	if db.o.Sync {
		if err := unix.Msync(old.m[:old.tail], unix.MS_SYNC); err != nil {
			return err
		}
		if err := db.syncState(); err != nil {
			return err
		}
	}
	c, err := db.newChunk(old.epoch+1, db.version, db.root)
	if err != nil {
		return err
	}
	db.cur = c
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
	return unix.Msync(db.tree, unix.MS_SYNC)
}
