package pagerdb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"
)

// EpochStore holds sealed epoch files. Put and Delete must be durable when
// they return. Open must return an immutable snapshot until its reader closes,
// and an error matching os.ErrNotExist when an epoch is absent.
type EpochStore interface {
	// Put publishes an epoch durably before returning.
	Put(epoch uint64, r io.Reader) error
	// Open returns immutable epoch bytes and their size.
	Open(epoch uint64) (io.ReaderAt, int64, error)
	// Latest returns the highest stored epoch, or zero for an empty store.
	Latest() uint64
	// Delete removes an epoch durably. Rewind deletes from newest to oldest.
	Delete(epoch uint64) error
}

// LocalStore keeps one file per epoch in a directory.
type LocalStore struct {
	dir    string
	latest atomic.Uint64
}

// NewLocalStore opens or creates a directory for sealed epoch files.
func NewLocalStore(dir string) (*LocalStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	s := &LocalStore{dir: dir}
	for _, de := range des {
		if e, err := strconv.ParseUint(de.Name(), 10, 64); err == nil && e > s.latest.Load() {
			s.latest.Store(e)
		}
	}
	return s, nil
}

func (s *LocalStore) path(epoch uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%012d", epoch))
}

// Put atomically publishes a durable epoch file.
func (s *LocalStore) Put(epoch uint64, r io.Reader) error {
	tmp := s.path(epoch) + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err = io.Copy(f, r); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, s.path(epoch))
	}
	if err == nil {
		err = syncDir(s.dir)
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	if epoch > s.latest.Load() {
		s.latest.Store(epoch)
	}
	return nil
}

// Open opens an epoch for random reads. The returned reader implements io.Closer.
func (s *LocalStore) Open(epoch uint64) (io.ReaderAt, int64, error) {
	f, err := os.Open(s.path(epoch))
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, fi.Size(), nil
}

// Latest returns the highest stored epoch number, or zero.
func (s *LocalStore) Latest() uint64 { return s.latest.Load() }

// PGEPOCH4 stores backward XOR changes. Header fields are seven little-endian
// u64 values after the magic: epoch, checkpoint version, end version, cumulative
// leaf count, index offset, root count, roots offset. The index starts with one
// u32 leaf key per row, then the rows. Index rows are 64 bytes:
// leaf u32, frames u32, directory offset u64, latest epoch u64, previous epoch
// u64, dense offset u64, records u64, base epoch u64, undo cost u32, zero u32.
// Inherited rows keep only leaf, latest epoch, base epoch and undo cost.
// Dense rows are version relative to checkpoint u32, offset u16, length u16.
// They are newest first, including reverse write order within one version.
// Frame directories are offset u64, first record u64, count u32, flags u32.
// Frame headers are flags u8, padding[3], stored length u32, decoded length u32,
// count u32, first record u64, oldest relative version u32, newest version u32.
// Payloads are dense-row headers followed by XOR bytes. A full run, and a final
// run that starts a new base, additionally ends with the 1024-byte leaf after
// undoing that run. Flag bits are zstd=1 and
// full-copy=2. Roots remain sorted (hash[32], version u64) records.
const (
	epochMagic = "PGEPOCH4"
	epochHdr   = 64
	frameHdr   = 32
	frameCap   = 16 << 10
	idxRec     = 64
	idxKey     = 4
	// baseHopCost charges each epoch hop as this many decoded bytes.
	baseHopCost = 256
	denseRec    = 8
	frameDirRec = 24
	rootRec     = 40
)

type sealedEpoch struct {
	epoch, start, end uint64
	ra                io.ReaderAt
	mem               []byte // read-only mapping when the store returns an *os.File
	nLeaves, indexOff uint64
	size              int64
	nRoots, rootsOff  uint64
}

type sealState struct {
	sealed      []*sealedEpoch
	sealQ       []*chunk
	sealPending int
	sealWaits   uint64
	sealWorkers []sealBuffers
	sealCond    sync.Cond
	sealWG      sync.WaitGroup
	sealStop    bool
	sealDone    chan struct{}
	sealErr     error
}

// enqueueSeal hands a closed chunk to the sealer. Caller holds db.mu.
func (db *DB) enqueueSeal(c *chunk) {
	db.sealWG.Add(1)
	db.sealPending++
	db.sealQ = append(db.sealQ, c)
	db.sealCond.Broadcast()
}

func (db *DB) sealer() {
	defer close(db.sealDone)
	defer func() {
		for i := range db.sealWorkers {
			if db.sealWorkers[i].encoder != nil {
				db.sealWorkers[i].encoder.Close()
			}
		}
		db.sealWorkers = nil
	}()
	for {
		db.mu.Lock()
		for len(db.sealQ) == 0 && !db.sealStop {
			db.sealCond.Wait()
		}
		if len(db.sealQ) == 0 {
			db.mu.Unlock()
			return
		}
		c := db.sealQ[0]
		db.sealQ = db.sealQ[1:]
		failed := db.sealErr != nil
		db.mu.Unlock()
		var err error
		if !failed {
			err = db.seal(c)
		}
		if c.endState != nil {
			c.endState.Close()
			c.endState = nil
		}
		db.mu.Lock()
		if err != nil {
			db.sealErr = fmt.Errorf("pagerdb: seal epoch %d: %w", c.epoch, err)
		}
		db.sealPending--
		db.sealCond.Broadcast()
		db.mu.Unlock()
		db.sealWG.Done()
	}
}

// syncDurable writes the state back and syncs it off the commit path. The sync
// starts after the current checkpoint, so every older chunk becomes removable.
// The state mapping is fixed while the sealer runs, so db.mu guards only cur.
func (db *DB) syncDurable() error {
	db.mu.Lock()
	target := db.cur.epoch
	db.mu.Unlock()
	fi, err := db.stateF.Stat()
	if err != nil {
		return err
	}
	if err = unix.Msync(db.state[:fi.Size()], unix.MS_SYNC); err != nil {
		return err
	}
	if err = db.stateF.Sync(); err != nil {
		return err
	}
	db.mu.Lock()
	db.durableEpoch = max(db.durableEpoch, target)
	db.mu.Unlock()
	return nil
}

func (db *DB) stopSealer() {
	db.mu.Lock()
	if db.sealDone == nil || db.sealStop {
		db.mu.Unlock()
		return
	}
	db.sealStop = true
	db.sealCond.Broadcast()
	db.mu.Unlock()
	<-db.sealDone
}

func (db *DB) closeSealed() {
	db.sealMu.Lock()
	defer db.sealMu.Unlock()
	for _, s := range db.sealed {
		s.close()
	}
	db.sealed = nil
}

// Seal closes the current epoch (if it has any version) and waits until every closed epoch is sealed.
func (db *DB) Seal() error {
	db.mu.Lock()
	if db.seq() != db.blockSeq || len(db.touches) > 0 {
		db.mu.Unlock()
		return ErrPending
	}
	var err error
	if db.version > db.cur.start {
		err = db.rotate()
	}
	db.mu.Unlock()
	if err != nil {
		return err
	}
	db.sealWG.Wait()
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.sealErr
}

// seal validates the chunk, then sorts undo references by leaf with bounded
// memory and writes trimmed newest-first XOR runs.
func (db *DB) seal(c *chunk) error {
	dir, err := os.MkdirTemp(db.dir, "seal-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	create := func(name string) (*os.File, error) { return os.CreateTemp(dir, name) }
	roots, err := create("roots-")
	if err != nil {
		return err
	}
	defer roots.Close()
	rw := bufio.NewWriterSize(roots, 64<<10)
	var rr [rootRec]byte
	copy(rr[:32], c.m[16:48])
	le.PutUint64(rr[32:], c.start)
	if _, err = rw.Write(rr[:]); err != nil {
		return err
	}
	end, nRoots, nPieces := c.start, uint64(1), uint64(0)
	minLeaf, maxLeaf := ^uint64(0), uint64(0)
	for pos := int64(cpRec); pos < c.tail; {
		r := c.m[pos:]
		switch r[0] {
		case 'E':
			n, ver, fo := uint64(le.Uint32(r[8:])), le.Uint64(r[16:]), le.Uint64(r[24:])
			if ver <= c.start || ver-c.start > uint64(^uint32(0)) {
				return fmt.Errorf("pagerdb: epoch version span exceeds uint32; reduce EpochVersions")
			}
			for p := uint64(0); p < n; {
				at := fo + p
				minLeaf, maxLeaf = min(minLeaf, at/LeafSize), max(maxLeaf, at/LeafSize)
				nPieces++
				p += min(n-p, LeafSize-at%LeafSize)
			}
			pos += entryHdr + 2*int64(n)
		case 'B':
			end = le.Uint64(r[8:])
			if end < c.start || end-c.start > uint64(^uint32(0)) {
				return fmt.Errorf("pagerdb: epoch version span exceeds uint32; reduce EpochVersions")
			}
			copy(rr[:32], r[16:48])
			le.PutUint64(rr[32:], end)
			if _, err = rw.Write(rr[:]); err != nil {
				return err
			}
			nRoots++
			pos += blockRec
		default:
			return fmt.Errorf("pagerdb: invalid log record while sealing")
		}
	}
	if err = rw.Flush(); err != nil {
		return err
	}
	var endState []byte
	if c.endState != nil {
		if fi, e := c.endState.Stat(); e == nil && fi.Size() > 0 {
			if endState, e = unix.Mmap(int(c.endState.Fd()), 0, int(fi.Size()), unix.PROT_READ, unix.MAP_SHARED); e != nil {
				endState = nil
			}
		}
		if endState != nil {
			defer unix.Munmap(endState)
		}
	}
	out, err := db.parallelSeal(dir, c, end, nRoots, nPieces, minLeaf, maxLeaf, roots, endState)
	if err != nil {
		return err
	}
	defer out.Close()
	if err = db.o.Store.Put(c.epoch, out); err != nil {
		return err
	}
	sealed, err := db.openSealed(c.epoch)
	if err != nil {
		return err
	}
	// The log is trimmed only after the state it would recover is durable. One
	// sync covers every kept chunk up to the current one, so later trims skip it.
	db.mu.Lock()
	needSync := len(db.chunks) > 1 && db.chunks[1].epoch > db.durableEpoch
	db.mu.Unlock()
	if needSync {
		if err = db.syncDurable(); err != nil {
			sealed.close()
			return err
		}
	}
	if err = c.f.Truncate(c.tail); err != nil {
		sealed.close()
		return err
	}
	db.mu.Lock()
	db.sealed = append(db.sealed, sealed)
	c.sealed = true
	dropped := db.dropChunks()
	db.mu.Unlock()
	db.closeChunks(dropped)
	return nil
}

// readWindowRange reconstructs one leaf range with fixed auxiliary memory.
// Caller holds db.mu. It reads mutable head bytes only if no later log covers
// them, so a reader never races an owner's Touch-announced Map write.
func (db *DB) readWindowRange(version, leaf, offset uint64, out []byte) {
	off := leaf*LeafSize + offset
	end := off + uint64(len(out))
	p, ok := db.heads[uint32(leaf)]
	if !ok || p < db.pieceBase || db.entries[db.pieceEntry(db.pieces[p-db.pieceBase])-db.entryBase].ver <= version {
		copy(out, db.state[off:end])
		return
	}
	var covered [LeafSize / 64]uint64 // bit i: out[i] came from an undo image
	for ok && p >= db.pieceBase {
		pc := db.pieces[p-db.pieceBase]
		e := &db.entries[db.pieceEntry(pc)-db.entryBase]
		if e.ver <= version {
			break
		}
		m, eo, en := db.rec(e)
		lo, hi := max(eo, off), min(eo+en, end)
		if lo < hi {
			copy(out[lo-off:hi-off], m[entryHdr+lo-eo:])
			for i := lo - off; i < hi-off; {
				n := min(hi-off-i, 64-i%64)
				covered[i/64] |= ^uint64(0) >> (64 - n) << (i % 64)
				i += n
			}
		}
		p, ok = db.prevPiece(p, pc)
	}
	for i := 0; i < len(out); {
		w := covered[i/64] >> (i % 64)
		rest := 64 - i%64
		if w&1 != 0 {
			i += min(bits.TrailingZeros64(^w), rest)
			continue
		}
		next := min(i+min(bits.TrailingZeros64(w), rest), len(out))
		copy(out[i:next], db.state[off+uint64(i):off+uint64(next)])
		i = next
	}
}

func (db *DB) openSealed(epoch uint64) (*sealedEpoch, error) {
	ra, size, err := db.o.Store.Open(epoch)
	if err != nil {
		return nil, err
	}
	// Local files are mapped: index lookups become memory reads without syscalls,
	// locks or copies, and the kernel page cache bounds their memory.
	var mem []byte
	if f, ok := ra.(*os.File); ok && size >= epochHdr && size <= int64(^uint(0)>>1) {
		mem, _ = unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	}
	s, err := parseEpoch(ra, size, epoch, mem)
	if err != nil {
		if mem != nil {
			unix.Munmap(mem)
		}
		if c, ok := ra.(io.Closer); ok {
			c.Close()
		}
	}
	return s, err
}

// close releases the epoch's mapping and reader.
func (s *sealedEpoch) close() {
	if s.mem != nil {
		unix.Munmap(s.mem)
		s.mem = nil
	}
	if c, ok := s.ra.(io.Closer); ok {
		c.Close()
	}
}

func parseEpoch(ra io.ReaderAt, size int64, epoch uint64, mem []byte) (*sealedEpoch, error) {
	var h [epochHdr]byte
	if _, err := ra.ReadAt(h[:], 0); err != nil {
		return nil, err
	}
	if string(h[:8]) != epochMagic || le.Uint64(h[8:]) != epoch {
		return nil, fmt.Errorf("pagerdb: epoch %d: bad header", epoch)
	}
	s := &sealedEpoch{epoch: epoch, start: le.Uint64(h[16:]), end: le.Uint64(h[24:]), ra: ra, mem: mem, size: size, nLeaves: le.Uint64(h[32:]), indexOff: le.Uint64(h[40:]), nRoots: le.Uint64(h[48:]), rootsOff: le.Uint64(h[56:])}
	if size < epochHdr || s.start > s.end || s.end-s.start > uint64(^uint32(0)) || s.nRoots != s.end-s.start+1 || s.indexOff < epochHdr || s.indexOff > uint64(size) || s.nLeaves > (uint64(size)-s.indexOff)/(idxKey+idxRec) || s.rootsOff != s.indexOff+s.nLeaves*(idxKey+idxRec) || s.rootsOff > uint64(size) || s.nRoots > (uint64(size)-s.rootsOff)/rootRec || s.rootsOff+s.nRoots*rootRec != uint64(size) {
		return nil, fmt.Errorf("pagerdb: epoch %d: invalid bounds", epoch)
	}
	var prevLeaf uint32
	for i := uint64(0); i < s.nLeaves; i++ {
		r, e := s.indexRecord(i)
		if e != nil {
			return nil, e
		}
		key, e := s.indexKey(i)
		if e != nil {
			return nil, e
		}
		leaf, frames, dir, last, prior, dense, n := le.Uint32(r[:]), le.Uint32(r[4:]), le.Uint64(r[8:]), le.Uint64(r[16:]), le.Uint64(r[24:]), le.Uint64(r[32:]), le.Uint64(r[40:])
		base, cost := le.Uint64(r[48:]), le.Uint32(r[56:])
		if key != leaf || i > 0 && leaf <= prevLeaf || last == 0 || last > epoch || base > last || cost >= frameCap || base == last && cost != 0 || le.Uint32(r[60:]) != 0 || frames == 0 && (dir != 0 || last == epoch || prior != 0 || dense != 0 || n != 0) || frames > 0 && (last != epoch || prior >= epoch || n == 0 || dir < epochHdr || dir > s.indexOff || uint64(frames) > (s.indexOff-dir)/frameDirRec || dense < epochHdr || dense > s.indexOff || n > (s.indexOff-dense)/denseRec) {
			return nil, fmt.Errorf("pagerdb: epoch %d: invalid cumulative index", epoch)
		}
		prevLeaf = leaf
		if frames == 0 {
			continue
		}
		expected := uint64(0)
		for j := uint32(0); j < frames; j++ {
			d, e := s.frameRecord(r, j)
			if e != nil {
				return nil, e
			}
			at, first, count, flags := le.Uint64(d[:]), le.Uint64(d[8:]), le.Uint32(d[16:]), le.Uint32(d[20:])
			if at < epochHdr || at > s.indexOff || s.indexOff-at < frameHdr || first != expected || count == 0 || uint64(count) > n-expected || flags > 3 || j+1 < frames && flags&2 == 0 {
				return nil, fmt.Errorf("pagerdb: invalid XOR frame directory")
			}
			expected += uint64(count)
			if j+1 == frames && (base == epoch) != (flags&2 != 0) {
				return nil, fmt.Errorf("pagerdb: XOR base copy mismatch")
			}
		}
		if expected != n {
			return nil, fmt.Errorf("pagerdb: XOR directory count mismatch")
		}
		prev := ^uint32(0)
		for j := uint64(0); j < n; j++ {
			d, e := s.denseRecord(r, j)
			if e != nil {
				return nil, e
			}
			v, o, k := le.Uint32(d[:]), uint64(le.Uint16(d[4:])), uint64(le.Uint16(d[6:]))
			if v == 0 || uint64(v) > s.end-s.start || v > prev || k == 0 || o+k > LeafSize {
				return nil, fmt.Errorf("pagerdb: invalid dense XOR index")
			}
			prev = v
		}
	}
	var prevRoot [32]byte
	var r [rootRec]byte
	for i := uint64(0); i < s.nRoots; i++ {
		if _, e := ra.ReadAt(r[:], int64(s.rootsOff+i*rootRec)); e != nil {
			return nil, e
		}
		v := le.Uint64(r[32:])
		if v < s.start || v > s.end || i > 0 && bytes.Compare(prevRoot[:], r[:32]) > 0 {
			return nil, fmt.Errorf("pagerdb: epoch %d: invalid root record", epoch)
		}
		copy(prevRoot[:], r[:32])
	}
	return s, nil
}

// The process-wide cache for stores that are not local files retains at most
// 512 index pages (2 MiB).
var epochIndexCache struct {
	sync.Mutex
	slots [512]struct {
		owner *sealedEpoch
		at    uint64
		bytes [4096]byte
	}
}

func (s *sealedEpoch) readSmall(at uint64, out []byte) error {
	if at > uint64(s.size) || uint64(len(out)) > uint64(s.size)-at {
		return io.ErrUnexpectedEOF
	}
	if s.mem != nil {
		copy(out, s.mem[at:])
		return nil
	}
	epochIndexCache.Lock()
	defer epochIndexCache.Unlock()
	for len(out) > 0 {
		page, within := at/4096, at%4096
		slot := &epochIndexCache.slots[((page+s.epoch<<32)*0x9e3779b97f4a7c15)>>55]
		if slot.owner != s || slot.at != page {
			slot.owner = nil
			n := min(uint64(len(slot.bytes)), uint64(s.size)-page*4096)
			if _, err := s.ra.ReadAt(slot.bytes[:n], int64(page*4096)); err != nil {
				return err
			}
			slot.owner = s
			slot.at = page
		}
		n := min(len(out), 4096-int(within))
		copy(out[:n], slot.bytes[within:within+uint64(n)])
		out = out[n:]
		at += uint64(n)
	}
	return nil
}
func (s *sealedEpoch) indexRecord(i uint64) ([idxRec]byte, error) {
	var r [idxRec]byte
	if i >= s.nLeaves {
		return r, fmt.Errorf("pagerdb: epoch index out of bounds")
	}
	err := s.readSmall(s.indexOff+s.nLeaves*idxKey+i*idxRec, r[:])
	return r, err
}

// indexKey reads the leaf of row i from the dense key column, so a lookup's
// binary search touches 4 bytes per probe instead of a whole row.
func (s *sealedEpoch) indexKey(i uint64) (uint32, error) {
	at := s.indexOff + i*idxKey
	if s.mem != nil {
		return le.Uint32(s.mem[at:]), nil
	}
	var k [idxKey]byte
	err := s.readSmall(at, k[:])
	return le.Uint32(k[:]), err
}

func (s *sealedEpoch) lookup(leaf uint64) ([idxRec]byte, bool, error) {
	var r [idxRec]byte
	lo, hi := uint64(0), s.nLeaves
	for lo < hi {
		m := lo + (hi-lo)/2
		k, e := s.indexKey(m)
		if e != nil {
			return r, false, e
		}
		if uint64(k) < leaf {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == s.nLeaves {
		return r, false, nil
	}
	if k, e := s.indexKey(lo); e != nil || uint64(k) != leaf {
		return r, false, e
	}
	r, e := s.indexRecord(lo)
	return r, e == nil, e
}

// loadSealed opens every sealed epoch of the store.
func (db *DB) loadSealed() error {
	for e := uint64(1); e <= db.o.Store.Latest(); e++ {
		s, err := db.openSealed(e)
		if err != nil {
			if len(db.sealed) == 0 && os.IsNotExist(err) {
				continue
			}
			return err
		}
		if len(db.sealed) > 0 && db.sealed[len(db.sealed)-1].end != s.start {
			s.close()
			return fmt.Errorf("pagerdb: sealed epoch checkpoint gap")
		}
		db.sealed = append(db.sealed, s)
	}
	return nil
}

// decodeXORFrame validates one bounded raw or compressed backward run. A
// compressed payload is decoded into dst's storage; a raw one is returned as is.
func decodeXORFrame(h, body, dst []byte) ([]byte, error) {
	if len(h) != frameHdr || h[0] > 3 || h[1] != 0 || h[2] != 0 || h[3] != 0 || len(body) != int(le.Uint32(h[4:])) || len(body) > frameCap+LeafSize {
		return nil, fmt.Errorf("pagerdb: invalid XOR frame header")
	}
	raw, count := le.Uint32(h[8:]), le.Uint32(h[12:])
	if raw > frameCap+LeafSize || count == 0 || count > frameCap/(denseRec+1) {
		return nil, fmt.Errorf("pagerdb: invalid XOR frame bounds")
	}
	if h[0]&1 != 0 {
		dec, err := getFrameDecoder()
		if err == nil {
			body, err = dec.DecodeAll(body, dst[:0])
			putFrameDecoder(dec)
		}
		if err != nil {
			return nil, fmt.Errorf("pagerdb: invalid compressed XOR frame: %w", err)
		}
	}
	if len(body) != int(raw) {
		return nil, fmt.Errorf("pagerdb: XOR decoded size mismatch")
	}
	records := len(body)
	if h[0]&2 != 0 {
		records -= LeafSize
	}
	if records < 0 || records > frameCap {
		return nil, fmt.Errorf("pagerdb: XOR record size")
	}
	p, prev := 0, ^uint32(0)
	for i := uint32(0); i < count; i++ {
		if records-p < denseRec {
			return nil, fmt.Errorf("pagerdb: truncated XOR record")
		}
		ver, off, n := le.Uint32(body[p:]), int(le.Uint16(body[p+4:])), int(le.Uint16(body[p+6:]))
		p += denseRec
		if ver == 0 || ver > prev || n == 0 || off+n > LeafSize || n > records-p || body[p] == 0 || body[p+n-1] == 0 {
			return nil, fmt.Errorf("pagerdb: invalid XOR record")
		}
		if i == 0 && ver != le.Uint32(h[28:]) {
			return nil, fmt.Errorf("pagerdb: invalid newest XOR version")
		}
		prev = ver
		p += n
	}
	if p != records || prev != le.Uint32(h[24:]) {
		return nil, fmt.Errorf("pagerdb: invalid XOR frame end")
	}
	return body, nil
}

// frameDecoders keeps idle zstd decoders for concurrent sealed reads.
var frameDecoders = make(chan *zstd.Decoder, 64)

func getFrameDecoder() (*zstd.Decoder, error) {
	select {
	case d := <-frameDecoders:
		return d, nil
	default:
		return zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(2<<20), zstd.WithDecoderMaxWindow(1<<20))
	}
}

func putFrameDecoder(d *zstd.Decoder) {
	select {
	case frameDecoders <- d:
	default:
		d.Close()
	}
}

// frameBuffers holds decoded-payload buffers so sealed reads do not allocate.
var frameBuffers = sync.Pool{New: func() any { b := make([]byte, 0, frameCap+LeafSize); return &b }}

func applyXORRecords(body []byte, count uint32, threshold uint32, offset uint64, out []byte) error {
	p := 0
	for i := uint32(0); i < count; i++ {
		if len(body)-p < denseRec {
			return fmt.Errorf("pagerdb: truncated XOR record")
		}
		ver, off, n := le.Uint32(body[p:]), uint64(le.Uint16(body[p+4:])), uint64(le.Uint16(body[p+6:]))
		p += denseRec
		if n == 0 || off+n > LeafSize || n > uint64(len(body)-p) {
			return fmt.Errorf("pagerdb: invalid XOR range")
		}
		if ver > threshold {
			lo, hi := max(off, offset), min(off+n, offset+uint64(len(out)))
			for j := lo; j < hi; j++ {
				out[j-offset] ^= body[p+int(j-off)]
			}
		}
		p += int(n)
	}
	return nil
}

func (s *sealedEpoch) denseRecord(row [idxRec]byte, i uint64) ([denseRec]byte, error) {
	var r [denseRec]byte
	if i >= le.Uint64(row[40:]) {
		return r, fmt.Errorf("pagerdb: dense index out of bounds")
	}
	err := s.readSmall(le.Uint64(row[32:])+i*denseRec, r[:])
	return r, err
}
func (s *sealedEpoch) frameRecord(row [idxRec]byte, i uint32) ([frameDirRec]byte, error) {
	var r [frameDirRec]byte
	if i >= le.Uint32(row[4:]) {
		return r, fmt.Errorf("pagerdb: frame directory out of bounds")
	}
	err := s.readSmall(le.Uint64(row[8:])+uint64(i)*frameDirRec, r[:])
	return r, err
}

// readFrame returns a frame header and its decoded payload. The payload aliases
// the epoch mapping or dst's storage; it is valid until dst is reused.
func (s *sealedEpoch) readFrame(dir [frameDirRec]byte, dst []byte) ([frameHdr]byte, []byte, error) {
	var h [frameHdr]byte
	off := le.Uint64(dir[:])
	if off < epochHdr || off > s.indexOff || s.indexOff-off < frameHdr {
		return h, nil, fmt.Errorf("pagerdb: frame offset out of bounds")
	}
	if err := s.readSmall(off, h[:]); err != nil {
		return h, nil, err
	}
	n := uint64(le.Uint32(h[4:]))
	if n > frameCap+LeafSize || n > s.indexOff-off-frameHdr {
		return h, nil, fmt.Errorf("pagerdb: frame payload out of bounds")
	}
	if le.Uint64(h[16:]) != le.Uint64(dir[8:]) || le.Uint32(h[12:]) != le.Uint32(dir[16:]) || uint32(h[0]) != le.Uint32(dir[20:]) {
		return h, nil, fmt.Errorf("pagerdb: frame directory mismatch")
	}
	var body []byte
	if s.mem != nil {
		body = s.mem[off+frameHdr : off+frameHdr+n]
	} else {
		if h[0]&1 != 0 {
			body = make([]byte, n) // compressed input; dst receives the output
		} else {
			body = dst[:n]
		}
		if _, err := s.ra.ReadAt(body, int64(off)+frameHdr); err != nil {
			return h, nil, err
		}
	}
	body, err := decodeXORFrame(h[:], body, dst)
	return h, body, err
}

// undoLeaf checks dense range metadata before loading any XOR payload. A complete
// run copy newer than the target replaces all earlier runs in this epoch.
func (s *sealedEpoch) undoLeaf(row [idxRec]byte, v, offset uint64, out []byte) error {
	buf := frameBuffers.Get().(*[]byte)
	defer frameBuffers.Put(buf)
	threshold := uint32(0)
	if v >= s.end {
		return nil
	}
	if v >= s.start {
		threshold = uint32(v - s.start)
	}
	n := le.Uint64(row[40:])
	lo, hi := uint64(0), n
	for lo < hi {
		m := lo + (hi-lo)/2
		r, e := s.denseRecord(row, m)
		if e != nil {
			return e
		}
		if le.Uint32(r[:]) > threshold {
			lo = m + 1
		} else {
			hi = m
		}
	}
	stop := lo
	if stop == 0 {
		return nil
	}
	overlap := false
	for i := uint64(0); i < stop; i++ {
		r, e := s.denseRecord(row, i)
		if e != nil {
			return e
		}
		a, b := uint64(le.Uint16(r[4:])), uint64(le.Uint16(r[6:]))
		if a < offset+uint64(len(out)) && a+b > offset {
			overlap = true
			break
		}
	}
	if !overlap {
		return nil
	}
	// Find the final complete run wholly newer than the target. Every non-final
	// run ends in a copy, so this is the nearest usable copy within this epoch.
	frames := le.Uint32(row[4:])
	fl, fh := uint32(0), frames
	for fl < fh {
		m := fl + (fh-fl)/2
		r, e := s.frameRecord(row, m)
		if e != nil {
			return e
		}
		if le.Uint64(r[8:])+uint64(le.Uint32(r[16:])) <= stop {
			fl = m + 1
		} else {
			fh = m
		}
	}
	begin := uint32(0)
	if fl > 0 {
		r, e := s.frameRecord(row, fl-1)
		if e != nil {
			return e
		}
		if le.Uint32(r[20:])&2 == 0 && fl > 1 {
			fl--
			r, e = s.frameRecord(row, fl-1)
			if e != nil {
				return e
			}
		}
		if le.Uint32(r[20:])&2 != 0 {
			_, body, e := s.readFrame(r, *buf)
			if e != nil {
				return e
			}
			copy(out, body[len(body)-LeafSize+int(offset):len(body)-LeafSize+int(offset)+len(out)])
			begin = fl
		}
	}
	for i := begin; i < frames; i++ {
		r, e := s.frameRecord(row, i)
		if e != nil {
			return e
		}
		if le.Uint64(r[8:]) >= stop {
			break
		}
		h, body, e := s.readFrame(r, *buf)
		if e != nil {
			return e
		}
		if e = applyXORRecords(body, le.Uint32(h[12:]), threshold, offset, out); e != nil {
			return e
		}
	}
	return nil
}

// sealedRange starts at the latest sealed boundary, then follows previous-change
// epoch pointers backwards. Dense index searches avoid unrelated byte ranges.
// The epoch walk runs under sealMu only, so deep reads do not block writers.
func (db *DB) sealedRange(leaf, v, offset uint64, out []byte) error {
	if offset+uint64(len(out)) > LeafSize {
		return fmt.Errorf("pagerdb: sealed range outside leaf")
	}
	db.mu.Lock()
	sealed := db.sealed
	if len(sealed) == 0 || v >= sealed[len(sealed)-1].end {
		db.readWindowRange(v, leaf, offset, out)
		db.mu.Unlock()
		return nil
	}
	if v < sealed[0].start {
		db.mu.Unlock()
		return ErrVersion
	}
	latest := sealed[len(sealed)-1]
	db.readWindowRange(latest.end, leaf, offset, out)
	// Lock order is db.mu then sealMu; writers of sealMu hold db.mu first.
	db.sealMu.RLock()
	db.mu.Unlock()
	defer db.sealMu.RUnlock()
	row, ok, err := latest.lookup(leaf)
	if err != nil || !ok {
		return err
	}
	// find returns nil for an epoch older than the retained history.
	find := func(epoch uint64) (*sealedEpoch, [idxRec]byte, error) {
		var row [idxRec]byte
		i := sort.Search(len(sealed), func(i int) bool { return sealed[i].epoch >= epoch })
		if i == len(sealed) || sealed[i].epoch != epoch {
			if epoch < sealed[0].epoch {
				return nil, row, nil
			}
			return nil, row, fmt.Errorf("pagerdb: missing previous XOR epoch %d", epoch)
		}
		return sealed[i], row, nil
	}
	// changed loads the row of a recorded change of leaf in s.
	changed := func(s *sealedEpoch) ([idxRec]byte, error) {
		row, ok, err := s.lookup(leaf)
		if err == nil && (!ok || le.Uint32(row[4:]) == 0 || le.Uint64(row[16:]) != s.epoch) {
			err = fmt.Errorf("pagerdb: missing XOR leaf in previous epoch")
		}
		return row, err
	}
	s, _, err := find(le.Uint64(row[16:]))
	if err != nil || s == nil {
		return err
	}
	if row, err = changed(s); err != nil {
		return err
	}
	// Jump along base copies to the oldest one at or after v; the undo walk
	// from there is bounded by the sealer's base policy.
	var from *sealedEpoch
	var fromRow [idxRec]byte
	for base := le.Uint64(row[48:]); base != 0; base = le.Uint64(row[48:]) {
		b, brow, err := find(base)
		if err != nil {
			return err
		}
		if b == nil || b.start < v {
			break
		}
		if brow, err = changed(b); err != nil {
			return err
		}
		if le.Uint64(brow[48:]) != base {
			return fmt.Errorf("pagerdb: XOR base epoch without copy")
		}
		from, fromRow = b, brow
		prior := le.Uint64(brow[24:])
		if prior == 0 {
			s = nil
			break
		}
		if s, _, err = find(prior); err != nil {
			return err
		}
		if s == nil {
			break
		}
		if row, err = changed(s); err != nil {
			return err
		}
	}
	if from != nil {
		if err = from.baseCopy(fromRow, offset, out); err != nil || from.start == v {
			return err
		}
	}
	for s != nil && s.end > v {
		if err = s.undoLeaf(row, v, offset, out); err != nil || s.start <= v {
			return err
		}
		prior := le.Uint64(row[24:])
		if prior == 0 {
			return nil
		}
		if s, _, err = find(prior); err != nil || s == nil {
			return err
		}
		if row, err = changed(s); err != nil {
			return err
		}
	}
	return nil
}

// baseCopy copies the leaf state at the epoch start from the final run's copy.
func (s *sealedEpoch) baseCopy(row [idxRec]byte, offset uint64, out []byte) error {
	d, err := s.frameRecord(row, le.Uint32(row[4:])-1)
	if err != nil {
		return err
	}
	if le.Uint32(d[20:])&2 == 0 {
		return fmt.Errorf("pagerdb: XOR base epoch without copy")
	}
	buf := frameBuffers.Get().(*[]byte)
	defer frameBuffers.Put(buf)
	_, body, err := s.readFrame(d, *buf)
	if err != nil {
		return err
	}
	tail := body[len(body)-LeafSize:]
	copy(out, tail[offset:])
	return nil
}

func (s *sealedEpoch) findRoot(root [32]byte) (uint64, bool, error) {
	var rec [rootRec]byte
	var readErr error
	i := sort.Search(int(s.nRoots), func(i int) bool {
		if _, e := s.ra.ReadAt(rec[:], int64(s.rootsOff)+int64(i)*rootRec); e != nil {
			readErr = e
		}
		return bytes.Compare(rec[:32], root[:]) > 0
	})
	if readErr != nil || i == 0 {
		return 0, false, readErr
	}
	if _, err := s.ra.ReadAt(rec[:], int64(s.rootsOff)+int64(i-1)*rootRec); err != nil {
		return 0, false, err
	}
	return le.Uint64(rec[32:]), [32]byte(rec[:32]) == root, nil
}

// Delete removes an epoch and makes the directory change durable.
func (s *LocalStore) Delete(epoch uint64) error {
	if err := os.Remove(s.path(epoch)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	if epoch == s.latest.Load() {
		latest := uint64(0)
		entries, err := os.ReadDir(s.dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			n, err := strconv.ParseUint(e.Name(), 10, 64)
			if err == nil && n > latest {
				latest = n
			}
		}
		s.latest.Store(latest)
	}
	return nil
}

func (s *sealedEpoch) eachChangedLeaf(f func(uint64) error) error {
	for i := uint64(0); i < s.nLeaves; i++ {
		r, err := s.indexRecord(i)
		if err != nil {
			return err
		}
		if le.Uint32(r[4:]) != 0 {
			if err = f(uint64(le.Uint32(r[:]))); err != nil {
				return err
			}
		}
	}
	return nil
}
