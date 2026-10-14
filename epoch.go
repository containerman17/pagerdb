package pagerdb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
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

// PGEPOCH3 stores backward XOR changes. Header fields are seven little-endian
// u64 values after the magic: epoch, checkpoint version, end version, cumulative
// leaf count, index offset, root count, roots offset. Index rows are 48 bytes:
// leaf u32, frames u32, directory offset u64, latest epoch u64, previous epoch
// u64, dense offset u64, records u64. Inherited rows keep only leaf/latest epoch.
// Dense rows are version relative to checkpoint u32, offset u16, length u16.
// They are newest first, including reverse write order within one version.
// Frame directories are offset u64, first record u64, count u32, flags u32.
// Frame headers are flags u8, padding[3], stored length u32, decoded length u32,
// count u32, first record u64, oldest relative version u32, newest version u32.
// Payloads are dense-row headers followed by XOR bytes. A full run additionally
// ends with the 1024-byte leaf after undoing that run. Flag bits are zstd=1 and
// full-copy=2. Roots remain sorted (hash[32], version u64) records.
const (
	epochMagic  = "PGEPOCH3"
	epochHdr    = 64
	frameHdr    = 32
	frameCap    = 256 << 10
	idxRec      = 48
	denseRec    = 8
	frameDirRec = 24
	rootRec     = 40
)

type sealedEpoch struct {
	epoch, start, end uint64
	ra                io.ReaderAt
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
	for _, s := range db.sealed {
		if c, ok := s.ra.(io.Closer); ok {
			c.Close()
		}
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

// seal sorts undo references on disk, then trims and writes newest-first XOR runs.
func (db *DB) seal(c *chunk) error {
	dir, err := os.MkdirTemp(db.dir, "seal-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	create := func(name string) (*os.File, error) { return os.CreateTemp(dir, name) }
	pieces, err := create("pieces-")
	if err != nil {
		return err
	}
	defer pieces.Close()
	roots, err := create("roots-")
	if err != nil {
		return err
	}
	defer roots.Close()
	pw, rw := bufio.NewWriterSize(pieces, 64<<10), bufio.NewWriterSize(roots, 64<<10)
	var rr [rootRec]byte
	copy(rr[:32], c.m[16:48])
	le.PutUint64(rr[32:], c.start)
	if _, err = rw.Write(rr[:]); err != nil {
		return err
	}
	end, nRoots := c.start, uint64(1)
	minLeaf, maxLeaf := ^uint64(0), uint64(0)
	var ref [40]byte
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
				k := min(n-p, LeafSize-at%LeafSize)
				oldAt := uint64(pos) + entryHdr + p
				for i, v := range []uint64{at / LeafSize, uint64(pos) + p, ver, oldAt} {
					le.PutUint64(ref[i*8:], v)
				}
				le.PutUint16(ref[32:], uint16(at%LeafSize))
				le.PutUint16(ref[34:], uint16(k))
				if _, err = pw.Write(ref[:]); err != nil {
					return err
				}
				p += k
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
	if err = pw.Flush(); err != nil {
		return err
	}
	if err = rw.Flush(); err != nil {
		return err
	}
	out, err := db.parallelSeal(dir, c, end, nRoots, minLeaf, maxLeaf, pieces, roots)
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
	db.mu.Lock()
	defer db.mu.Unlock()
	db.sealed = append(db.sealed, sealed)
	c.sealed = true
	db.dropChunks()
	return nil
}

// readWindowRange reconstructs one leaf range with fixed auxiliary memory.
// Caller holds db.mu. It reads mutable head bytes only if no later log covers them.
func (db *DB) readWindowRange(version, leaf, offset uint64, out []byte) {
	var covered [LeafSize]bool
	off := leaf*LeafSize + offset
	end := off + uint64(len(out))
	p, ok := db.heads[uint32(leaf)]
	for ok && p >= db.pieceBase {
		pc := db.pieces[p-db.pieceBase]
		e := db.entries[pc.entry-db.entryBase]
		if e.ver <= version {
			break
		}
		lo, hi := max(e.off, off), min(e.off+uint64(e.n), end)
		if lo < hi {
			copy(out[lo-off:hi-off], e.c.m[e.pos+entryHdr+int64(lo-e.off):])
			for i := lo - off; i < hi-off; i++ {
				covered[i] = true
			}
		}
		p = pc.prev
	}
	for i := 0; i < len(out); {
		if covered[i] {
			i++
			continue
		}
		start := i
		for i < len(out) && !covered[i] {
			i++
		}
		copy(out[start:i], db.state[off+uint64(start):off+uint64(i)])
	}
}

func (db *DB) openSealed(epoch uint64) (*sealedEpoch, error) {
	ra, size, err := db.o.Store.Open(epoch)
	if err != nil {
		return nil, err
	}
	s, err := parseEpoch(ra, size, epoch)
	if err != nil {
		if c, ok := ra.(io.Closer); ok {
			c.Close()
		}
	}
	return s, err
}

func parseEpoch(ra io.ReaderAt, size int64, epoch uint64) (*sealedEpoch, error) {
	var h [epochHdr]byte
	if _, err := ra.ReadAt(h[:], 0); err != nil {
		return nil, err
	}
	if string(h[:8]) != epochMagic || le.Uint64(h[8:]) != epoch {
		return nil, fmt.Errorf("pagerdb: epoch %d: bad header", epoch)
	}
	s := &sealedEpoch{epoch: epoch, start: le.Uint64(h[16:]), end: le.Uint64(h[24:]), ra: ra, size: size, nLeaves: le.Uint64(h[32:]), indexOff: le.Uint64(h[40:]), nRoots: le.Uint64(h[48:]), rootsOff: le.Uint64(h[56:])}
	if size < epochHdr || s.start > s.end || s.end-s.start > uint64(^uint32(0)) || s.nRoots != s.end-s.start+1 || s.indexOff < epochHdr || s.indexOff > uint64(size) || s.nLeaves > (uint64(size)-s.indexOff)/idxRec || s.rootsOff != s.indexOff+s.nLeaves*idxRec || s.rootsOff > uint64(size) || s.nRoots > (uint64(size)-s.rootsOff)/rootRec || s.rootsOff+s.nRoots*rootRec != uint64(size) {
		return nil, fmt.Errorf("pagerdb: epoch %d: invalid bounds", epoch)
	}
	var prevLeaf uint32
	for i := uint64(0); i < s.nLeaves; i++ {
		r, e := s.indexRecord(i)
		if e != nil {
			return nil, e
		}
		leaf, frames, dir, last, prior, dense, n := le.Uint32(r[:]), le.Uint32(r[4:]), le.Uint64(r[8:]), le.Uint64(r[16:]), le.Uint64(r[24:]), le.Uint64(r[32:]), le.Uint64(r[40:])
		if i > 0 && leaf <= prevLeaf || last == 0 || last > epoch || frames == 0 && (dir != 0 || last == epoch || prior != 0 || dense != 0 || n != 0) || frames > 0 && (last != epoch || prior >= epoch || n == 0 || dir < epochHdr || dir > s.indexOff || uint64(frames) > (s.indexOff-dir)/frameDirRec || dense < epochHdr || dense > s.indexOff || n > (s.indexOff-dense)/denseRec) {
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

// The process-wide cache retains at most 64 index pages (256 KiB).
var epochIndexCache struct {
	sync.Mutex
	slots [64]struct {
		owner *sealedEpoch
		at    uint64
		bytes [4096]byte
	}
}

func (s *sealedEpoch) readSmall(at uint64, out []byte) error {
	if at > uint64(s.size) || uint64(len(out)) > uint64(s.size)-at {
		return io.ErrUnexpectedEOF
	}
	epochIndexCache.Lock()
	defer epochIndexCache.Unlock()
	for len(out) > 0 {
		page, within := at/4096, at%4096
		slot := &epochIndexCache.slots[(page+s.epoch*17)%uint64(len(epochIndexCache.slots))]
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
	err := s.readSmall(s.indexOff+i*idxRec, r[:])
	return r, err
}

func (s *sealedEpoch) lookup(leaf uint64) ([idxRec]byte, bool, error) {
	lo, hi := uint64(0), s.nLeaves
	for lo < hi {
		m := lo + (hi-lo)/2
		r, e := s.indexRecord(m)
		if e != nil {
			return r, false, e
		}
		if uint64(le.Uint32(r[:])) < leaf {
			lo = m + 1
		} else {
			hi = m
		}
	}
	var r [idxRec]byte
	if lo == s.nLeaves {
		return r, false, nil
	}
	r, e := s.indexRecord(lo)
	return r, uint64(le.Uint32(r[:])) == leaf, e
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
			if c, ok := s.ra.(io.Closer); ok {
				c.Close()
			}
			return fmt.Errorf("pagerdb: sealed epoch checkpoint gap")
		}
		db.sealed = append(db.sealed, s)
	}
	return nil
}

// decodeXORFrame validates one bounded raw or compressed backward run.
func decodeXORFrame(h, body []byte) ([]byte, error) {
	if len(h) != frameHdr || h[0] > 3 || h[1] != 0 || h[2] != 0 || h[3] != 0 || len(body) != int(le.Uint32(h[4:])) || len(body) > frameCap+LeafSize {
		return nil, fmt.Errorf("pagerdb: invalid XOR frame header")
	}
	raw, count := le.Uint32(h[8:]), le.Uint32(h[12:])
	if raw > frameCap+LeafSize || count == 0 || count > frameCap/(denseRec+1) {
		return nil, fmt.Errorf("pagerdb: invalid XOR frame bounds")
	}
	if h[0]&1 != 0 {
		frameDecoder.Lock()
		var err error
		if frameDecoder.dec == nil {
			frameDecoder.dec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(2<<20), zstd.WithDecoderMaxWindow(1<<20))
		}
		if err == nil {
			body, err = frameDecoder.dec.DecodeAll(body, nil)
		}
		frameDecoder.Unlock()
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
		if records < frameCap-(denseRec+LeafSize)+1 {
			return nil, fmt.Errorf("pagerdb: premature run copy")
		}
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

var frameDecoder struct {
	sync.Mutex
	dec *zstd.Decoder
}

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
func (s *sealedEpoch) readFrame(dir [frameDirRec]byte) ([frameHdr]byte, []byte, error) {
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
	body := make([]byte, n)
	if _, err := s.ra.ReadAt(body, int64(off)+frameHdr); err != nil {
		return h, nil, err
	}
	body, err := decodeXORFrame(h[:], body)
	return h, body, err
}

// undoLeaf checks dense range metadata before loading any XOR payload. A complete
// run copy newer than the target replaces all earlier runs in this epoch.
func (s *sealedEpoch) undoLeaf(row [idxRec]byte, v, offset uint64, out []byte) error {
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
			_, body, e := s.readFrame(r)
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
		h, body, e := s.readFrame(r)
		if e != nil {
			return e
		}
		if e = applyXORRecords(body, le.Uint32(h[12:]), threshold, offset, out); e != nil {
			return e
		}
	}
	return nil
}

func (db *DB) sealedLeaf(leaf, v uint64, out []byte) error { return db.sealedRange(leaf, v, 0, out) }

// sealedRange starts at the latest sealed boundary, then follows previous-change
// epoch pointers backwards. Dense index searches avoid unrelated byte ranges.
func (db *DB) sealedRange(leaf, v, offset uint64, out []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if offset+uint64(len(out)) > LeafSize {
		return fmt.Errorf("pagerdb: sealed range outside leaf")
	}
	if len(db.sealed) == 0 || v >= db.sealed[len(db.sealed)-1].end {
		db.readWindowRange(v, leaf, offset, out)
		return nil
	}
	if v < db.sealed[0].start {
		return ErrVersion
	}
	latest := db.sealed[len(db.sealed)-1]
	db.readWindowRange(latest.end, leaf, offset, out)
	row, ok, err := latest.lookup(leaf)
	if err != nil || !ok {
		return err
	}
	epoch := le.Uint64(row[16:])
	for epoch != 0 {
		i := sort.Search(len(db.sealed), func(i int) bool { return db.sealed[i].epoch >= epoch })
		if i == len(db.sealed) || db.sealed[i].epoch != epoch {
			if epoch < db.sealed[0].epoch {
				return nil
			}
			return fmt.Errorf("pagerdb: missing previous XOR epoch %d", epoch)
		}
		s := db.sealed[i]
		if s.end <= v {
			return nil
		}
		row, ok, err = s.lookup(leaf)
		if err != nil {
			return err
		}
		if !ok || le.Uint32(row[4:]) == 0 || le.Uint64(row[16:]) != epoch {
			return fmt.Errorf("pagerdb: missing XOR leaf in previous epoch")
		}
		if err = s.undoLeaf(row, v, offset, out); err != nil {
			return err
		}
		if s.start <= v {
			return nil
		}
		epoch = le.Uint64(row[24:])
	}
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
