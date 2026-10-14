package pagerdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

// EpochStore holds sealed epoch files. Put must be durable when it returns.
type EpochStore interface {
	Put(epoch uint64, r io.Reader) error
	Open(epoch uint64) (io.ReaderAt, int64, error)
	Latest() uint64
}

// LocalStore keeps one file per epoch in a directory.
type LocalStore struct {
	dir    string
	latest atomic.Uint64
}

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

func (s *LocalStore) Latest() uint64 { return s.latest.Load() }

// Epoch file:
//
//	header (64 B): "PGEPOCH1" | epoch | start version | end version | leaves | index offset | roots | roots offset
//	frames, grouped by leaf in leaf order:
//	  flags u8 (compression, 0 = none) _[3] | entry bytes u32 | first version u64 | base leaf [1024] | entries
//	  entry: uvarint version delta | offset u16 | length u16 | bytes
//	index: per changed leaf, sorted: leaf u32 | frames u32 | first frame offset u64
//	leaf hashes: per changed leaf in index order, sha256 of the leaf at the epoch end
//	roots: (root [32], version u64) for every version in [start, end], sorted by root
//
// The base of a frame is the leaf before its first entry; versions are in (start, end].
const (
	epochMagic = "PGEPOCH1"
	epochHdr   = 64
	frameHdr   = 16
	frameCap   = 256 << 10
	idxRec     = 16
	rootRec    = 40
)

// shortcut: every sealed epoch's index stays in RAM (16 B per changed leaf), load it lazily
// once the number of epochs grows.
type sealedEpoch struct {
	epoch, start, end uint64
	ra                io.ReaderAt
	idx               []byte
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

type sealPiece struct {
	leaf, ver uint64
	off       uint16
	data      []byte
}

// seal groups a closed chunk's entries by leaf into frames, writes the epoch file through the
// store, then lets the chunk leave the window.
func (db *DB) seal(c *chunk) error {
	var ps []sealPiece
	roots := le.AppendUint64(append([]byte(nil), c.m[16:48]...), c.start)
	end := c.start
	for pos := int64(cpRec); pos < c.tail; {
		r := c.m[pos:]
		switch r[0] {
		case 'E':
			n, ver, fo := uint64(le.Uint32(r[8:])), le.Uint64(r[16:]), le.Uint64(r[24:])
			data := r[entryHdr+n : entryHdr+2*n]
			for p := uint64(0); p < n; {
				o := fo + p
				k := min(n-p, LeafSize-o%LeafSize)
				ps = append(ps, sealPiece{leaf: o / LeafSize, ver: ver, off: uint16(o % LeafSize), data: data[p : p+k]})
				p += k
			}
			pos += entryHdr + 2*int64(n)
		case 'B':
			end = le.Uint64(r[8:])
			roots = append(roots, r[16:48]...)
			roots = le.AppendUint64(roots, end)
			pos += blockRec
		default:
			return fmt.Errorf("bad record %q at %d", r[0], pos)
		}
	}
	sort.SliceStable(ps, func(i, j int) bool { return ps[i].leaf < ps[j].leaf })

	// shortcut: the epoch file is built in RAM, stream it through a temp file once epochs reach GBs.
	var buf bytes.Buffer
	buf.Write(make([]byte, epochHdr))
	var idx, hashes []byte
	base := make([]byte, LeafSize)
	var tmp [binary.MaxVarintLen64 + 4]byte
	for i := 0; i < len(ps); {
		leaf := ps[i].leaf
		db.mu.Lock()
		db.readWindow(c.start, leaf*LeafSize, base)
		db.mu.Unlock()
		idx = le.AppendUint32(idx, uint32(leaf))
		nFramesAt := len(idx)
		idx = le.AppendUint32(idx, 0)
		idx = le.AppendUint64(idx, uint64(buf.Len()))
		frames, frameAt, entryBytes, prev := 0, 0, 0, uint64(0)
		for ; i < len(ps) && ps[i].leaf == leaf; i++ {
			p := ps[i]
			enc := binary.PutUvarint(tmp[:], p.ver-prev)
			sz := enc + 4 + len(p.data)
			if frames == 0 || entryBytes+sz > frameCap {
				if frames > 0 {
					le.PutUint32(buf.Bytes()[frameAt+4:], uint32(entryBytes))
				}
				frameAt, entryBytes, prev, frames = buf.Len(), 0, p.ver, frames+1
				var h [frameHdr]byte
				le.PutUint64(h[8:], p.ver)
				buf.Write(h[:])
				buf.Write(base)
				enc = binary.PutUvarint(tmp[:], 0)
				sz = enc + 4 + len(p.data)
			}
			le.PutUint16(tmp[enc:], p.off)
			le.PutUint16(tmp[enc+2:], uint16(len(p.data)))
			buf.Write(tmp[:enc+4])
			buf.Write(p.data)
			entryBytes += sz
			prev = p.ver
			copy(base[p.off:], p.data)
		}
		le.PutUint32(buf.Bytes()[frameAt+4:], uint32(entryBytes))
		le.PutUint32(idx[nFramesAt:], uint32(frames))
		h := sha256.Sum256(base)
		hashes = append(hashes, h[:]...)
	}
	n := len(roots) / rootRec
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		return bytes.Compare(roots[order[a]*rootRec:order[a]*rootRec+32], roots[order[b]*rootRec:order[b]*rootRec+32]) < 0
	})
	indexOff := uint64(buf.Len())
	buf.Write(idx)
	buf.Write(hashes)
	rootsOff := uint64(buf.Len())
	for _, o := range order {
		buf.Write(roots[o*rootRec : (o+1)*rootRec])
	}
	h := buf.Bytes()[:epochHdr]
	copy(h, epochMagic)
	for i, v := range []uint64{c.epoch, c.start, end, uint64(len(idx) / idxRec), indexOff, uint64(n), rootsOff} {
		le.PutUint64(h[8+8*i:], v)
	}
	if err := db.o.Store.Put(c.epoch, bytes.NewReader(buf.Bytes())); err != nil {
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
	ra, _, err := db.o.Store.Open(epoch)
	if err != nil {
		return nil, err
	}
	var h [epochHdr]byte
	if _, err := ra.ReadAt(h[:], 0); err != nil {
		return nil, err
	}
	if string(h[:8]) != epochMagic || le.Uint64(h[8:]) != epoch {
		return nil, fmt.Errorf("pagerdb: epoch %d: bad header", epoch)
	}
	s := &sealedEpoch{epoch: epoch, start: le.Uint64(h[16:]), end: le.Uint64(h[24:]), ra: ra,
		nRoots: le.Uint64(h[48:]), rootsOff: le.Uint64(h[56:])}
	s.idx = make([]byte, le.Uint64(h[32:])*idxRec)
	if _, err := ra.ReadAt(s.idx, int64(le.Uint64(h[40:]))); err != nil {
		return nil, err
	}
	return s, nil
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
	n := len(s.idx) / idxRec
	i := sort.Search(n, func(i int) bool { return uint64(le.Uint32(s.idx[i*idxRec:])) >= leaf })
	if i == n || uint64(le.Uint32(s.idx[i*idxRec:])) != leaf {
		return false, nil
	}
	frames, off := le.Uint32(s.idx[i*idxRec+4:]), int64(le.Uint64(s.idx[i*idxRec+8:]))
	var h [frameHdr]byte
	at, eb, first := int64(0), uint32(0), uint64(0)
	for f := uint32(0); f < frames; f++ {
		if _, err := s.ra.ReadAt(h[:], off); err != nil {
			return true, err
		}
		fv := le.Uint64(h[8:])
		if f > 0 && fv > v {
			break
		}
		at, eb, first = off, le.Uint32(h[4:]), fv
		off += frameHdr + LeafSize + int64(le.Uint32(h[4:]))
	}
	fr := make([]byte, LeafSize+int(eb))
	if _, err := s.ra.ReadAt(fr, at+frameHdr); err != nil {
		return true, err
	}
	copy(out, fr[:LeafSize])
	ver := first
	for p := LeafSize; p < len(fr); {
		d, k := binary.Uvarint(fr[p:])
		ver += d
		if ver > v {
			break
		}
		o, l := le.Uint16(fr[p+k:]), int(le.Uint16(fr[p+k+2:]))
		copy(out[o:], fr[p+k+4:p+k+4+l])
		p += k + 4 + l
	}
	return true, nil
}

func (s *sealedEpoch) findRoot(root [32]byte) (uint64, bool, error) {
	var rec [rootRec]byte
	var err error
	i := sort.Search(int(s.nRoots), func(i int) bool {
		if _, e := s.ra.ReadAt(rec[:], int64(s.rootsOff)+int64(i)*rootRec); e != nil {
			err = e
		}
		return bytes.Compare(rec[:32], root[:]) >= 0
	})
	if err != nil || i == int(s.nRoots) {
		return 0, false, err
	}
	if _, err := s.ra.ReadAt(rec[:], int64(s.rootsOff)+int64(i)*rootRec); err != nil {
		return 0, false, err
	}
	return le.Uint64(rec[32:]), [32]byte(rec[:32]) == root, nil
}

// sealedLeaf reads a leaf at version v below the window: the first sealed epoch from v's epoch
// on that changed the leaf has its bytes at v; if none did, the window has them at the last
// sealed end.
func (db *DB) sealedLeaf(leaf, v uint64, out []byte) error {
	db.mu.Lock()
	sealed := db.sealed
	db.mu.Unlock()
	i := sort.Search(len(sealed), func(i int) bool { return sealed[i].end >= v })
	for {
		for ; i < len(sealed); i++ {
			if ok, err := sealed[i].leaf(leaf, v, out); ok || err != nil {
				return err
			}
		}
		db.mu.Lock()
		if len(db.sealed) == len(sealed) {
			db.readWindow(sealed[len(sealed)-1].end, leaf*LeafSize, out)
			db.mu.Unlock()
			return nil
		}
		sealed = db.sealed
		db.mu.Unlock()
	}
}
