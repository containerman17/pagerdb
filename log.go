package pagerdb

import (
	"fmt"
	"hash/crc32"
	"math"
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
	// endState is a clone of the state file at the chunk's last version,
	// taken at rotation when the filesystem supports cloning. The sealer
	// reads epoch-end leaves from it without db.mu.
	endState *os.File
}

// entry indexes one log record of the window. The state offset and length
// stay in the mapped record, and the struct has no pointers, so the garbage
// collector never scans the index.
type entry struct {
	ver uint64
	loc uint64 // record position in its chunk << 24 | low 24 bits of the chunk epoch
}

func (e *entry) epoch() uint32 { return uint32(e.loc) & (1<<24 - 1) }

// piece links an entry into the per-leaf chain (newest first) the views walk.
// Both links are 32-bit: the window holds fewer than 2^32 entries and pieces.
type piece struct {
	entry uint32 // low bits of the entry seq
	back  uint32 // distance to the previous piece of the leaf, 0 if none
	leaf  uint32
}

// rec returns e's mapped log record and the state range it covers.
func (db *DB) rec(e *entry) (m []byte, off, n uint64) {
	m = db.chunks[(e.epoch()-uint32(db.chunks[0].epoch))&(1<<24-1)].m[e.loc>>24:]
	return m, le.Uint64(m[24:]), uint64(le.Uint32(m[8:]))
}

// pieceEntry returns the entry seq of pc.
func (db *DB) pieceEntry(pc piece) int64 {
	return db.entryBase + int64(pc.entry-uint32(db.entryBase))
}

// prevPiece returns the previous piece seq of the leaf of piece p, if it is in the window.
func (db *DB) prevPiece(p int64, pc piece) (int64, bool) {
	prev := p - int64(pc.back)
	return prev, pc.back != 0 && prev >= db.pieceBase
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
	// durableEpoch is the newest chunk whose checkpoint the synced state file
	// covers. Unclean recovery redoes every kept chunk, so only chunks older
	// than durableEpoch may be removed.
	durableEpoch uint64
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
	return c, nil
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
	copy(e[entryHdr:entryHdr+n], db.state[fo:])
	if b != nil {
		copy(e[entryHdr+n:], b)
	}
	le.PutUint32(e[12:], entryChecksum(e, n))
	e[0] = 'E'
	if b != nil {
		copy(db.state[fo:fo+n], b)
	}
	db.markDirty(fo, n)
	db.index(db.cur, pos, db.version+1, fo, n)
	return pos
}

func (db *DB) put(r Region, fo uint64, b []byte) { db.logEntry(r, fo, uint64(len(b)), b) }

func (db *DB) index(c *chunk, pos int64, ver, fo, n uint64) {
	seq := db.seq()
	if len(db.entries) >= math.MaxUint32 || len(db.pieces) >= math.MaxUint32-LeafSize {
		panic("pagerdb: window holds 2^32 log entries; reduce RetainVersions")
	}
	if pos >= 1<<40 {
		panic("pagerdb: log chunk reached 1 TiB; reduce EpochBytes")
	}
	db.entries = append(db.entries, entry{ver: ver, loc: uint64(pos)<<24 | c.epoch&(1<<24-1)})
	for l := fo / LeafSize; l <= (fo+n-1)/LeafSize; l++ {
		p := db.pieceSeq()
		back := uint32(0)
		if prev, ok := db.heads[uint32(l)]; ok && p-prev <= math.MaxUint32 {
			back = uint32(p - prev)
		}
		db.heads[uint32(l)] = p
		db.pieces = append(db.pieces, piece{entry: uint32(seq), back: back, leaf: uint32(l)})
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

// sealClone is cloneState; tests replace it to exercise the window fallback.
var sealClone = cloneState

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
	// Nothing is fsynced here: the sealer syncs the state in the background and
	// keeps every chunk recovery needs until then. The writeback makes the clone
	// see mapped writes; without cloning, the sealer reconstructs epoch-end
	// leaves from the window.
	old := db.cur
	if err := unix.Msync(db.state[:db.hdr(hNextSeg)*SegSize], unix.MS_SYNC); err != nil {
		return err
	}
	old.endState, _ = sealClone(db)
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
	if err := unix.Msync(db.tree, unix.MS_SYNC); err != nil {
		return err
	}
	// On macOS File.Sync is F_FULLFSYNC, which flushes the whole drive cache, so
	// a plain fsync for the state file followed by one full sync covers both.
	if err := unix.Fsync(int(db.stateF.Fd())); err != nil {
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
