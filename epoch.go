package pagerdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

// EpochStore holds sealed epoch files. Put and Delete must be durable when
// they return. Open must return an immutable snapshot until its reader closes.
type EpochStore interface {
	Put(epoch uint64, r io.Reader) error
	Open(epoch uint64) (io.ReaderAt, int64, error)
	Latest() uint64
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

// Epoch files use PGEPOCH2. The 64-byte header contains magic, epoch,
// start, end, cumulative index count, index offset, root count, root offset.
// Frames have a 16-byte header: flags u8, three zero bytes, stored payload
// length u32, first version u64. Flag 1 is zstd, flag 0 is raw. The decoded
// payload is a 1024-byte base followed by version-delta uvarint, offset u16,
// length u16, and replacement bytes. Payloads never exceed 263168 bytes.
// The sorted cumulative index has 24-byte records: leaf u32, frame count
// u32, frame offset u64, latest changed epoch u64. Inherited records have
// zero frame count and offset. Roots are sorted 40-byte (hash, version) rows.
// All fixed-width integers are little endian.
const (
	epochMagic = "PGEPOCH2"
	epochHdr   = 64
	frameHdr   = 16
	frameCap   = 256 << 10
	idxRec     = 24
	rootRec    = 40
)

type sealedEpoch struct {
	epoch, start, end uint64
	ra                io.ReaderAt
	nLeaves, indexOff uint64
	size              int64
	nRoots, rootsOff  uint64
}

type sealState struct {
	sealed   []*sealedEpoch
	sealQ    []*chunk
	sealCond sync.Cond
	sealWG   sync.WaitGroup
	sealStop bool
	sealDone chan struct{}
	sealErr  error
}

// enqueueSeal hands a closed chunk to the sealer. Caller holds db.mu.
func (db *DB) enqueueSeal(c *chunk) {
	db.sealWG.Add(1)
	db.sealQ = append(db.sealQ, c)
	db.sealCond.Signal()
}

func (db *DB) sealer() {
	defer close(db.sealDone)
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
		if !failed {
			if err := db.seal(c); err != nil {
				db.mu.Lock()
				db.sealErr = fmt.Errorf("pagerdb: seal epoch %d: %w", c.epoch, err)
				db.mu.Unlock()
			}
		}
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
	db.sealCond.Signal()
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

// seal sorts compact log references on disk, then streams bounded frames to a
// temporary file. Only the current frame and merge buffers occupy heap memory.
func (db *DB) seal(c *chunk) error {
	dir, err := os.MkdirTemp(db.dir, "seal-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	pieces, err := os.CreateTemp(dir, "pieces-")
	if err != nil {
		return err
	}
	defer pieces.Close()
	piecesWriter := bufio.NewWriterSize(pieces, 64<<10)
	roots, err := os.CreateTemp(dir, "roots-")
	if err != nil {
		return err
	}
	defer roots.Close()
	rootsWriter := bufio.NewWriterSize(roots, 64<<10)
	root := le.AppendUint64(append([]byte(nil), c.m[16:48]...), c.start)
	if _, err = rootsWriter.Write(root); err != nil {
		return err
	}
	end, nRoots := c.start, uint64(1)
	for pos := int64(cpRec); pos < c.tail; {
		r := c.m[pos:]
		switch r[0] {
		case 'E':
			n, ver, fo := uint64(le.Uint32(r[8:])), le.Uint64(r[16:]), le.Uint64(r[24:])
			for p := uint64(0); p < n; {
				o := fo + p
				k := min(n-p, LeafSize-o%LeafSize)
				var rec [40]byte
				for i, v := range []uint64{o / LeafSize, uint64(pos) + p, ver, uint64(pos) + entryHdr + n + p} {
					le.PutUint64(rec[i*8:], v)
				}
				le.PutUint16(rec[32:], uint16(o%LeafSize))
				le.PutUint16(rec[34:], uint16(k))
				if _, err = piecesWriter.Write(rec[:]); err != nil {
					return err
				}
				p += k
			}
			pos += entryHdr + 2*int64(n)
		case 'B':
			end = le.Uint64(r[8:])
			root = le.AppendUint64(append(root[:0], r[16:48]...), end)
			if _, err = rootsWriter.Write(root); err != nil {
				return err
			}
			nRoots++
			pos += blockRec
		default:
			return fmt.Errorf("bad record at %d", pos)
		}
	}
	if err = piecesWriter.Flush(); err != nil {
		return err
	}
	if err = rootsWriter.Flush(); err != nil {
		return err
	}
	sorted, err := diskSort(dir, pieces, 40, pieceLess)
	if err != nil {
		return err
	}
	defer sorted.Close()
	sortedReader := bufio.NewReaderSize(sorted, 64<<10)
	sortedRoots, err := diskSort(dir, roots, rootRec, rootLess)
	if err != nil {
		return err
	}
	defer sortedRoots.Close()
	out, err := os.CreateTemp(dir, "epoch-")
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err = out.Write(make([]byte, epochHdr)); err != nil {
		return err
	}
	index, err := os.CreateTemp(dir, "index-")
	if err != nil {
		return err
	}
	defer index.Close()
	indexWriter := bufio.NewWriterSize(index, 64<<10)
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return err
	}
	defer enc.Close()
	var rec [40]byte
	_, readErr := io.ReadFull(sortedReader, rec[:])
	var previous *sealedEpoch
	db.mu.Lock()
	if len(db.sealed) > 0 {
		previous = db.sealed[len(db.sealed)-1]
	}
	db.mu.Unlock()
	oldAt, nLeaves := uint64(0), uint64(0)
	copyOld := func(until uint64) error {
		if previous == nil {
			return nil
		}
		for oldAt < previous.nLeaves {
			r, e := previous.indexRecord(oldAt)
			if e != nil {
				return e
			}
			if uint64(le.Uint32(r[:])) >= until {
				break
			}
			clear(r[4:16])
			if _, e = indexWriter.Write(r[:]); e != nil {
				return e
			}
			oldAt++
			nLeaves++
		}
		return nil
	}
	base := make([]byte, LeafSize)
	for readErr == nil {
		leaf := le.Uint64(rec[:])
		if err = copyOld(leaf); err != nil {
			return err
		}
		if previous != nil && oldAt < previous.nLeaves {
			r, e := previous.indexRecord(oldAt)
			if e != nil {
				return e
			}
			if uint64(le.Uint32(r[:])) == leaf {
				oldAt++
			}
		}
		db.mu.Lock()
		db.readWindow(c.start, leaf*LeafSize, base)
		db.mu.Unlock()
		off, _ := out.Seek(0, io.SeekCurrent)
		frames := uint32(0)
		payload := append(make([]byte, 0, LeafSize+frameCap), base...)
		first, prev := le.Uint64(rec[16:]), le.Uint64(rec[16:])
		flush := func() error {
			var h [frameHdr]byte
			body := payload
			compressed := enc.EncodeAll(payload, nil)
			if len(compressed)*10 <= len(payload)*9 {
				body = compressed
				h[0] = 1
			}
			le.PutUint32(h[4:], uint32(len(body)))
			le.PutUint64(h[8:], first)
			if _, e := out.Write(h[:]); e != nil {
				return e
			}
			_, e := out.Write(body)
			frames++
			return e
		}
		for readErr == nil && le.Uint64(rec[:]) == leaf {
			ver, at := le.Uint64(rec[16:]), le.Uint64(rec[24:])
			o, k := le.Uint16(rec[32:]), le.Uint16(rec[34:])
			var tmp [14]byte
			n := binary.PutUvarint(tmp[:], ver-prev)
			if len(payload)+n+4+int(k) > LeafSize+frameCap {
				if err = flush(); err != nil {
					return err
				}
				payload = append(payload[:0], base...)
				first = ver
				n = binary.PutUvarint(tmp[:], 0)
			}
			le.PutUint16(tmp[n:], o)
			le.PutUint16(tmp[n+2:], k)
			payload = append(payload, tmp[:n+4]...)
			payload = append(payload, c.m[at:at+uint64(k)]...)
			copy(base[o:], c.m[at:at+uint64(k)])
			prev = ver
			_, readErr = io.ReadFull(sortedReader, rec[:])
		}
		if err = flush(); err != nil {
			return err
		}
		var ir [idxRec]byte
		le.PutUint32(ir[:], uint32(leaf))
		le.PutUint32(ir[4:], frames)
		le.PutUint64(ir[8:], uint64(off))
		le.PutUint64(ir[16:], c.epoch)
		if _, err = indexWriter.Write(ir[:]); err != nil {
			return err
		}
		nLeaves++
	}
	if readErr != io.EOF {
		return readErr
	}
	if err = copyOld(^uint64(0)); err != nil {
		return err
	}
	if err = indexWriter.Flush(); err != nil {
		return err
	}
	indexOff, _ := out.Seek(0, io.SeekCurrent)
	index.Seek(0, 0)
	if _, err = io.Copy(out, index); err != nil {
		return err
	}
	rootsOff, _ := out.Seek(0, io.SeekCurrent)
	if _, err = io.Copy(out, sortedRoots); err != nil {
		return err
	}
	var h [epochHdr]byte
	copy(h[:], epochMagic)
	for i, v := range []uint64{c.epoch, c.start, end, nLeaves, uint64(indexOff), nRoots, uint64(rootsOff)} {
		le.PutUint64(h[8+8*i:], v)
	}
	if _, err = out.WriteAt(h[:], 0); err != nil {
		return err
	}
	out.Seek(0, 0)
	if err = db.o.Store.Put(c.epoch, out); err != nil {
		return err
	}
	s, err := db.openSealed(c.epoch)
	if err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.sealed = append(db.sealed, s)
	c.sealed = true
	db.dropChunks()
	return nil
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
	if size < epochHdr || s.start > s.end || s.end-s.start == ^uint64(0) || s.nRoots != s.end-s.start+1 || s.indexOff < epochHdr || s.indexOff > uint64(size) || s.nLeaves > (uint64(size)-s.indexOff)/idxRec || s.rootsOff != s.indexOff+s.nLeaves*idxRec || s.rootsOff > uint64(size) || s.nRoots > (uint64(size)-s.rootsOff)/rootRec || s.rootsOff+s.nRoots*rootRec != uint64(size) {
		return nil, fmt.Errorf("pagerdb: epoch %d: invalid bounds", epoch)
	}
	var prevLeaf uint32
	for i := uint64(0); i < s.nLeaves; i++ {
		r, e := s.indexRecord(i)
		if e != nil {
			return nil, e
		}
		leaf, frames, off, last := le.Uint32(r[:]), le.Uint32(r[4:]), le.Uint64(r[8:]), le.Uint64(r[16:])
		if i > 0 && leaf <= prevLeaf || last == 0 || last > epoch || frames == 0 && (off != 0 || last == epoch) || frames > 0 && (last != epoch || off < epochHdr || off >= s.indexOff || uint64(frames) > (s.indexOff-off)/frameHdr) {
			return nil, fmt.Errorf("pagerdb: epoch %d: invalid index record %d", epoch, i)
		}
		prevLeaf = leaf
	}
	var prevRoot [32]byte
	for i := uint64(0); i < s.nRoots; i++ {
		var r [rootRec]byte
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

func (s *sealedEpoch) indexRecord(i uint64) ([idxRec]byte, error) {
	var r [idxRec]byte
	if i >= s.nLeaves {
		return r, fmt.Errorf("pagerdb: epoch index out of bounds")
	}
	epochIndexCache.Lock()
	defer epochIndexCache.Unlock()
	at := i * idxRec
	for done := 0; done < idxRec; {
		page := at / 4096
		within := at % 4096
		slot := &epochIndexCache.slots[(page+s.epoch*17)%uint64(len(epochIndexCache.slots))]
		if slot.owner != s || slot.at != page {
			slot.owner = nil
			n := min(uint64(len(slot.bytes)), s.nLeaves*idxRec-page*4096)
			if _, err := s.ra.ReadAt(slot.bytes[:n], int64(s.indexOff+page*4096)); err != nil {
				return r, err
			}
			slot.owner = s
			slot.at = page
		}
		n := min(idxRec-done, 4096-int(within))
		copy(r[done:done+n], slot.bytes[within:within+uint64(n)])
		done += n
		at += uint64(n)
	}
	return r, nil
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
			return err
		}
		db.sealed = append(db.sealed, s)
	}
	return nil
}

// leaf writes the leaf at version v into out if the leaf changed in this epoch. The frame is
// the last one starting at or before v (or the first); its entries up to v are applied.
func (s *sealedEpoch) leaf(leaf, v uint64, out []byte) (bool, error) {
	r, ok, err := s.lookup(leaf)
	if err != nil || !ok {
		return ok, err
	}
	frames, off := le.Uint32(r[4:]), le.Uint64(r[8:])
	if frames == 0 {
		return false, nil
	}
	var h [frameHdr]byte
	at := off
	for f := uint32(0); f < frames; f++ {
		if off < epochHdr || off > s.indexOff || s.indexOff-off < frameHdr {
			return true, fmt.Errorf("pagerdb: invalid frame offset")
		}
		if _, err = s.ra.ReadAt(h[:], int64(off)); err != nil {
			return true, err
		}
		if le.Uint64(h[8:]) > v && f > 0 {
			break
		}
		at = off
		sz := uint64(le.Uint32(h[4:]))
		if sz > LeafSize+frameCap || sz > s.indexOff-off-frameHdr {
			return true, fmt.Errorf("pagerdb: invalid frame size")
		}
		off += frameHdr + sz
	}
	if _, err = s.ra.ReadAt(h[:], int64(at)); err != nil {
		return true, err
	}
	body := make([]byte, le.Uint32(h[4:]))
	if _, err = s.ra.ReadAt(body, int64(at)+frameHdr); err != nil {
		return true, err
	}
	return true, decodeFrame(h[:], body, v, out)
}

// A shared decoder bounds retained decoder memory, including concurrent views.
var frameDecoder struct {
	sync.Mutex
	dec *zstd.Decoder
}

func decodeFrame(h, body []byte, v uint64, out []byte) error {
	if len(h) != frameHdr || len(out) < LeafSize || h[0] > 1 || h[1] != 0 || h[2] != 0 || h[3] != 0 || len(body) != int(le.Uint32(h[4:])) || len(body) > LeafSize+frameCap {
		return fmt.Errorf("pagerdb: invalid frame header")
	}
	if h[0] == 1 {
		var err error
		frameDecoder.Lock()
		if frameDecoder.dec == nil {
			frameDecoder.dec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(2<<20), zstd.WithDecoderMaxWindow(1<<20))
		}
		if err == nil {
			body, err = frameDecoder.dec.DecodeAll(body, nil)
		}
		frameDecoder.Unlock()
		if err != nil {
			return fmt.Errorf("pagerdb: invalid compressed frame: %w", err)
		}
	}
	if len(body) < LeafSize || len(body) > LeafSize+frameCap {
		return fmt.Errorf("pagerdb: invalid frame payload")
	}
	copy(out, body[:LeafSize])
	ver := le.Uint64(h[8:])
	for p := LeafSize; p < len(body); {
		d, k := binary.Uvarint(body[p:])
		if k <= 0 || d > ^uint64(0)-ver || len(body)-p-k < 4 {
			return fmt.Errorf("pagerdb: invalid frame version")
		}
		ver += d
		o, n := int(le.Uint16(body[p+k:])), int(le.Uint16(body[p+k+2:]))
		p += k + 4
		if n == 0 || o+n > LeafSize || n > len(body)-p {
			return fmt.Errorf("pagerdb: invalid frame entry bounds")
		}
		if ver <= v {
			copy(out[o:o+n], body[p:p+n])
		}
		p += n
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

// Reconstructed leaves are immutable. Pointer identity separates rewound histories.
var epochLeafCache struct {
	sync.Mutex
	slots [64]struct {
		owner         *sealedEpoch
		leaf, version uint64
		bytes         [LeafSize]byte
	}
}

// sealedLeaf binary-searches the cumulative indexes for the next epoch that
// changed the leaf. Its frame base reconstructs the requested version.
func (db *DB) sealedLeaf(leaf, v uint64, out []byte) (retErr error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.sealed) > 0 {
		owner := db.sealed[len(db.sealed)-1]
		slot := &epochLeafCache.slots[(leaf*31+v*17)%uint64(len(epochLeafCache.slots))]
		epochLeafCache.Lock()
		if slot.owner == owner && slot.leaf == leaf && slot.version == v {
			copy(out, slot.bytes[:])
			epochLeafCache.Unlock()
			return nil
		}
		epochLeafCache.Unlock()
		defer func() {
			if retErr == nil {
				epochLeafCache.Lock()
				slot.owner = owner
				slot.leaf = leaf
				slot.version = v
				copy(slot.bytes[:], out)
				epochLeafCache.Unlock()
			}
		}()
	}
	target := sort.Search(len(db.sealed), func(i int) bool { return db.sealed[i].end >= v })
	if target == len(db.sealed) {
		db.readWindow(v, leaf*LeafSize, out)
		return nil
	}
	threshold := db.sealed[target].epoch
	lo, hi := target, len(db.sealed)
	for lo < hi {
		m := lo + (hi-lo)/2
		r, ok, err := db.sealed[m].lookup(leaf)
		if err != nil {
			return err
		}
		if ok && le.Uint64(r[16:]) >= threshold {
			hi = m
		} else {
			lo = m + 1
		}
	}
	if lo < len(db.sealed) {
		ok, err := db.sealed[lo].leaf(leaf, v, out)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("pagerdb: inconsistent cumulative epoch index")
		}
		return nil
	}
	db.readWindow(db.sealed[len(db.sealed)-1].end, leaf*LeafSize, out)
	return nil
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
