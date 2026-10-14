package pagerdb

import (
	"bytes"
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

func (db *DB) mark() Mark {
	db.resetUndoKeys()
	return Mark{db.version, db.seq(), db.cur.tail, len(db.touches)}
}

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
func (db *DB) undoTo(pos, seq int64) { db.undoEntries(pos, seq, true) }

func (db *DB) undoEntries(pos, seq int64, cut bool) {
	db.resetUndoKeys()
	for i := db.seq() - 1; i >= seq; i-- {
		m, off, n := db.rec(db.entries.at(i))
		copy(db.state[off:off+n], m[entryHdr:entryHdr+n])
		db.markDirty(off, n)
		if cut {
			m[0] = 0
		}
		for db.pieces.len() > 0 && db.pieceEntry(*db.pieces.at(db.pieces.end - 1)) >= i {
			p, pc := db.pieces.end-1, *db.pieces.at(db.pieces.end - 1)
			if prev, ok := db.prevPiece(p, pc); ok {
				db.heads.set(pc.leaf, prev)
			} else {
				db.heads.del(pc.leaf)
			}
			db.pieces.end--
		}
		db.entries.end = i
	}
	if cut {
		clear(db.cur.m[pos:db.cur.tail])
	}
	db.cur.tail = pos
	clear(db.segCache)
	db.lastR = 0
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
	if !validCheckpoint(m) {
		return 0, cpRoot, nil, nil, false
	}
	cpVer, cpRoot = le.Uint64(m[8:]), [32]byte(m[16:48])
	pos, start, ver := int64(cpRec), int64(cpRec), cpVer
	var cur []int64
loop:
	for pos < int64(len(m)) {
		switch m[pos] {
		case 'E', 'U':
			if pos+entryHdr > int64(len(m)) {
				break loop
			}
			if le.Uint32(m[pos+8:]) == 0 {
				break loop
			}
			sz := entryHdr + 2*int64(le.Uint32(m[pos+8:]))
			if pos+sz > int64(len(m)) {
				break loop
			}
			if m[pos] == 'E' && le.Uint32(m[pos+4:]) != entryChecksum(m[pos:pos+sz], uint64(le.Uint32(m[pos+8:]))) {
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
				le.Uint32(r[48:]) != crc32.Update(crc32.Checksum(m[start:pos], castagnoli), castagnoli, r[4:48]) {
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
	return &chunk{epoch: epoch, f: f, m: m, size: fi.Size()}, nil
}

// open recovers the store. After an unclean shutdown every kept chunk is replayed oldest
// first (complete blocks forward with new bytes, the torn tail of the last chunk backwards
// with old bytes), the tree is rebuilt and the root must match the last block record.
// Older unsealed chunks are loaded into the window and queued for sealing.
func (db *DB) open() error {
	_, stateErr := os.Stat(filepath.Join(db.dir, "state"))
	mark, _ := os.ReadFile(filepath.Join(db.dir, "clean"))
	db.clean = len(mark) == cleanRec && string(mark[:8]) == cleanMagic && le.Uint32(mark[48:]) == crc32.Checksum(mark[:48], castagnoli)
	if db.clean {
		db.cleanVer, db.cleanRoot = le.Uint64(mark[8:]), [32]byte(mark[16:48])
	}
	db.unclean = stateErr == nil && !db.clean
	// Removing the marker is durable before any mapped mutation.
	if err := os.Remove(filepath.Join(db.dir, "clean")); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := syncDir(db.dir); err != nil {
		return err
	}
	db.heads = make(headTab, (db.maxLeaves+1<<16-1)>>16)
	db.rootIdx = map[[32]byte]uint64{}
	db.pins = map[uint64]int{}
	db.sealCond.L = &db.mu
	db.sealDone = make(chan struct{})

	if err := db.recoverRewind(); err != nil {
		return err
	}
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
		if c != nil && validCheckpoint(c.m) {
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
		if _, err := os.Stat(filepath.Join(db.dir, "state")); err == nil {
			return fmt.Errorf("pagerdb: recovery: state exists but checkpoint log is missing")
		}
		if db.o.Store.Latest() != 0 {
			return fmt.Errorf("pagerdb: %s: sealed epochs but no live log", db.dir)
		}
		if err := db.mapFiles(true); err != nil {
			return err
		}
		db.root = db.hashDirty()
		if err := db.syncState(); err != nil {
			return err
		}
		if err := syncDir(db.dir); err != nil {
			return err
		}
		c, err := db.newChunk(1, 0, db.root)
		if err != nil {
			return err
		}
		db.cur, db.chunks = c, []*chunk{c}
		db.blocks = []block{{ver: 0, root: db.root, c: c, end: cpRec}}
		db.rootIdx[db.root] = 0
		db.blockStart = cpRec
		db.durableEpoch = 1
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
	// A torn new checkpoint can make a previously sealed chunk current again.
	// Remove its sealed image before accepting new writes to that epoch.
	for len(db.sealed) > 0 && db.sealed[len(db.sealed)-1].epoch >= last.epoch {
		s := db.sealed[len(db.sealed)-1]
		if err := db.o.Store.Delete(s.epoch); err != nil {
			last.close()
			return err
		}
		s.close()
		db.sealed = db.sealed[:len(db.sealed)-1]
	}
	latest := db.o.Store.Latest()
	var queue []*chunk
	for _, e := range epochs[:len(epochs)-1] {
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
		if n := len(db.blocks); n > 0 && (db.blocks[n-1].ver != cpVer || db.blocks[n-1].root != cpRoot || db.chunks[len(db.chunks)-1].epoch+1 != e) {
			c.close()
			last.close()
			return fmt.Errorf("pagerdb: recovery: log gap before chunk %d", e)
		}
		c.start = cpVer
		// The sealer truncates the file; a crash may leave preallocated space.
		c.tail = cpRec
		if len(bl) > 0 {
			c.tail = bl[len(bl)-1].end
		}
		db.chunks = append(db.chunks, c)
		db.addBlocks(c, cpVer, cpRoot, bl)
		if e <= latest {
			c.sealed = true
		} else {
			queue = append(queue, c)
		}
	}
	if n := len(db.blocks); n > 0 {
		if v, r, _, _, _ := scan(last.m[:cpRec]); db.blocks[n-1].ver != v || db.blocks[n-1].root != r || db.chunks[len(db.chunks)-1].epoch+1 != last.epoch {
			last.close()
			return fmt.Errorf("pagerdb: recovery: log gap before chunk %d", last.epoch)
		}
	}
	db.chunks = append(db.chunks, last)
	db.cur = last
	if err := db.replay(last); err != nil {
		return err
	}
	db.durableEpoch = last.epoch
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
	// After a clean close the state and tree files were synced at the head the
	// marker names. If the log agrees and has no torn tail, trust the stored
	// tree root instead of rehashing the whole state.
	if db.clean && len(tail) == 0 {
		end, ver, root := int64(cpRec), cpVer, cpRoot
		if len(bl) > 0 {
			end, ver, root = bl[len(bl)-1].end, bl[len(bl)-1].ver, bl[len(bl)-1].root
		}
		if ver == db.cleanVer && root == db.cleanRoot && [32]byte(db.node(1, db.depth)) == root {
			db.version, db.root = ver, root
			c.tail = end
			db.addBlocks(c, cpVer, cpRoot, bl)
			db.blockStart, db.blockSeq = end, db.seq()
			return db.growState(db.hdr(hNextSeg))
		}
	}
	need := uint64(0)
	for i := db.entries.base; i < db.entries.end; i++ {
		_, off, n := db.rec(db.entries.at(i))
		if off > db.maxLeaves*LeafSize || n > db.maxLeaves*LeafSize-off {
			return fmt.Errorf("pagerdb: recovery: log state offset out of bounds")
		}
		need = max(need, off+n)
	}
	for _, b := range bl {
		for _, pos := range b.entries {
			off, n := le.Uint64(c.m[pos+24:]), uint64(le.Uint32(c.m[pos+8:]))
			if off > db.maxLeaves*LeafSize || n > db.maxLeaves*LeafSize-off {
				return fmt.Errorf("pagerdb: recovery: log state offset out of bounds")
			}
			need = max(need, off+n)
		}
	}
	for _, pos := range tail {
		off, n := le.Uint64(c.m[pos+24:]), uint64(le.Uint32(c.m[pos+8:]))
		if off > db.maxLeaves*LeafSize || n > db.maxLeaves*LeafSize-off {
			return fmt.Errorf("pagerdb: recovery: log state offset out of bounds")
		}
		need = max(need, off+n)
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
	}
	// The state file is synced at least as far as the oldest kept checkpoint,
	// so redoing every kept chunk in order reaches the last complete block.
	for i := db.entries.base; i < db.entries.end; i++ {
		m, off, n := db.rec(db.entries.at(i))
		copy(db.state[off:off+n], m[entryHdr+n:])
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
	c.tail = end
	db.addBlocks(c, cpVer, cpRoot, bl)
	db.blockStart, db.blockSeq = end, db.seq()
	if err := db.growState(db.hdr(hNextSeg)); err != nil {
		return err
	}
	root, err := db.rebuildTree()
	if err != nil {
		return err
	}
	if root != db.root {
		return fmt.Errorf("pagerdb: recovery: root %x does not match block %d root %x", root, db.version, db.root)
	}
	// Persist restored bytes before removing their undo records. Recovery itself
	// may lose power, so the undo records must remain usable until this point.
	if err := db.syncState(); err != nil {
		return err
	}
	clear(c.m[end:])
	return nil
}

// Rewind restores version v within RetainVersions, deletes newer sealed history,
// and starts a new history at v. Close views newer than v before calling it.
func (db *DB) Rewind(v uint64) error {
	db.sealWG.Wait()
	db.mu.Lock()
	defer db.mu.Unlock()
	if v > db.version {
		return ErrVersion
	}
	if (db.version > db.o.RetainVersions && v < db.version-db.o.RetainVersions) || v < max(db.chunks[0].start, db.windowStart) {
		return ErrWindow
	}
	for pv := range db.pins {
		if pv > v {
			return fmt.Errorf("pagerdb: rewind to %d with an open view at %d", v, pv)
		}
	}
	i := slices.IndexFunc(db.blocks, func(b block) bool { return b.ver == v })
	if i < 0 {
		return ErrWindow
	}
	b := db.blocks[i]
	if err := db.writeRewindIntent(v, b.c.epoch, b.end); err != nil {
		return err
	}
	db.indexPending()
	db.cur = b.c
	db.cur.sealed = false
	seq := db.seq()
	for seq > db.entries.base && db.entries.at(seq-1).ver > v {
		seq--
	}
	db.undoEntries(b.end, seq, false)
	if err := db.syncState(); err != nil {
		return err
	}
	if err := db.recoverRewind(); err != nil {
		return err
	}
	for len(db.sealed) > 0 && db.sealed[len(db.sealed)-1].epoch >= b.c.epoch {
		s := db.sealed[len(db.sealed)-1]
		if err := db.o.Store.Delete(s.epoch); err != nil {
			return err
		}
		db.sealMu.Lock()
		s.close()
		db.sealMu.Unlock()
		db.sealed = db.sealed[:len(db.sealed)-1]
	}
	if err := db.cur.setSize(int64(len(db.cur.m))); err != nil {
		return err
	}
	for len(db.chunks) > 0 && db.chunks[len(db.chunks)-1] != db.cur {
		c := db.chunks[len(db.chunks)-1]
		c.close()
		if err := os.Remove(db.chunkPath(c.epoch)); err != nil && !os.IsNotExist(err) {
			return err
		}
		db.chunks = db.chunks[:len(db.chunks)-1]
	}
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
	if err := db.syncAll(); err != nil {
		return err
	}
	// Epoch numbers restart at b.c, so durability restarts there too.
	db.durableEpoch = b.c.epoch
	return nil
}

func validCheckpoint(m []byte) bool {
	return len(m) >= cpRec && m[0] == 'C' && le.Uint32(m[48:]) == crc32.Checksum(m[8:48], castagnoli)
}

// rebuildTree runs only during Open. It replaces the disposable tree file with
// sparse zero storage and hashes nonzero leaves in bounded batches.
func (db *DB) rebuildTree() ([32]byte, error) {
	var zeroRoot [32]byte
	fi, err := db.stateF.Stat()
	if err != nil {
		return zeroRoot, err
	}
	if fi.Size() < 0 || uint64(fi.Size()) > db.maxLeaves*LeafSize || fi.Size()%LeafSize != 0 {
		return zeroRoot, fmt.Errorf("pagerdb: recovery: invalid state length")
	}
	if err = unix.Munmap(db.tree); err != nil {
		return zeroRoot, err
	}
	db.tree = nil
	if err = db.treeF.Truncate(0); err != nil {
		return zeroRoot, err
	}
	size := int64(treeStorageSize(db.maxLeaves))
	if err = db.treeF.Truncate(size); err != nil {
		return zeroRoot, err
	}
	db.tree, err = unix.Mmap(int(db.treeF.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return zeroRoot, err
	}
	clear(db.dirtyBit)
	const batch = 65536
	db.dirty = make([]uint32, 0, batch)
	var zero [LeafSize]byte
	leaves := uint64(fi.Size()) / LeafSize
	for start := uint64(0); start < leaves; start += batch {
		for leaf := start; leaf < min(start+batch, leaves); leaf++ {
			off := leaf * LeafSize
			if !bytes.Equal(db.state[off:off+LeafSize], zero[:]) {
				db.markDirty(off, LeafSize)
			}
		}
		db.hashDirty()
	}
	return [32]byte(db.node(1, db.depth)), nil
}
