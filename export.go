package pagerdb

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"
	"os"
	"path/filepath"
)

const imageMagic = "PGIMAGE2"
const imageHeader = 64

// imageTree folds sparse leaves without allocating a tree or hashing absent leaves.
type imageTree struct {
	stack [33][32]byte
	empty [33][32]byte
	count uint64
}

func newImageTree() *imageTree {
	t := &imageTree{}
	t.empty[0] = sha256.Sum256(make([]byte, LeafSize))
	for i := 1; i < len(t.empty); i++ {
		t.empty[i] = hashPair(t.empty[i-1][:], t.empty[i-1][:])
	}
	return t
}
func (t *imageTree) add(h [32]byte, level int) {
	n := t.count >> level
	t.count += uint64(1) << level
	for n&1 != 0 {
		h = hashPair(t.stack[level][:], h[:])
		level++
		n >>= 1
	}
	t.stack[level] = h
}
func (t *imageTree) zerosTo(end uint64) {
	for t.count < end {
		level := bits.Len64(end-t.count) - 1
		if t.count != 0 {
			level = min(level, bits.TrailingZeros64(t.count))
		}
		t.add(t.empty[level], level)
	}
}

// Export writes a sparse, self-describing image at version v. The writer receives
// the allocation metadata and nonzero leaves in increasing file-offset order.
// The caller must serialize Export with the owner's mutation API.
func (db *DB) Export(v uint64, w io.Writer) error {
	vw, err := db.View(v)
	if err != nil {
		return err
	}
	defer vw.Close()
	var head [LeafSize]byte
	if err = vw.readFile(0, head[:]); err != nil {
		return err
	}
	used := le.Uint64(head[hNextSeg:]) * SegSize / LeafSize
	if used > db.maxLeaves {
		return fmt.Errorf("pagerdb: export invalid state size")
	}
	// The root is computed from the image so exports also check the stored root.
	root, err := db.versionRoot(v)
	if err != nil {
		return err
	}
	var h [imageHeader]byte
	copy(h[:8], imageMagic)
	le.PutUint64(h[8:], db.maxLeaves)
	le.PutUint64(h[16:], v)
	le.PutUint64(h[24:], used)
	copy(h[32:], root[:])
	if err = writeFull(w, h[:]); err != nil {
		return err
	}
	t := newImageTree()
	var leaf [LeafSize]byte
	var idx [8]byte
	var zero [LeafSize]byte
	for i := uint64(0); i < used; i++ {
		if err = vw.readFile(i*LeafSize, leaf[:]); err != nil {
			return err
		}
		if bytes.Equal(leaf[:], zero[:]) {
			continue
		}
		t.zerosTo(i)
		t.add(sha256.Sum256(leaf[:]), 0)
		le.PutUint64(idx[:], i)
		if err = writeFull(w, idx[:]); err != nil {
			return err
		}
		if err = writeFull(w, leaf[:]); err != nil {
			return err
		}
	}
	t.zerosTo(db.maxLeaves)
	if t.stack[db.depth] != root {
		return fmt.Errorf("pagerdb: export root mismatch")
	}
	le.PutUint64(idx[:], ^uint64(0))
	return writeFull(w, idx[:])
}
func writeFull(w io.Writer, b []byte) error {
	n, err := w.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}
func (db *DB) versionRoot(v uint64) ([32]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, b := range db.blocks {
		if b.ver == v {
			return b.root, nil
		}
	}
	for _, s := range db.sealed {
		if v >= s.start && v <= s.end {
			var r [rootRec]byte
			for i := uint64(0); i < s.nRoots; i++ {
				if _, err := s.ra.ReadAt(r[:], int64(s.rootsOff+i*rootRec)); err != nil {
					return [32]byte{}, err
				}
				if le.Uint64(r[32:]) == v {
					return [32]byte(r[:32]), nil
				}
			}
		}
	}
	return [32]byte{}, ErrVersion
}

// Import creates a new store from an exported image, preserving its version and
// region IDs. It rejects existing destinations, malformed images and roots that
// differ from expectedRoot. Memory use is independent of image size.
func Import(dir string, r io.Reader, expectedRoot [32]byte) (*DB, error) {
	if _, err := os.Stat(dir); err == nil {
		return nil, fmt.Errorf("pagerdb: import destination exists")
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	stage, err := os.MkdirTemp(filepath.Dir(dir), ".pagerdb-import-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(stage)
	f, err := os.Create(filepath.Join(stage, "state"))
	if err != nil {
		return nil, err
	}
	capacity, version, root, err := readImage(r, f, expectedRoot)
	if err == nil {
		err = f.Sync()
	}
	cerr := f.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	if err = os.Mkdir(filepath.Join(stage, "log"), 0755); err != nil {
		return nil, err
	}
	var cp [cpRec]byte
	cp[0] = 'C'
	le.PutUint64(cp[8:], version)
	copy(cp[16:], root[:])
	le.PutUint32(cp[48:], crc32.Checksum(cp[8:48], castagnoli))
	if err = os.WriteFile(filepath.Join(stage, "log", "000000000001"), cp[:], 0644); err != nil {
		return nil, err
	}
	db, err := Open(stage, Options{MaxLeaves: capacity})
	if err != nil {
		return nil, err
	}
	if err = db.Close(); err != nil {
		return nil, err
	}
	if err = os.Rename(stage, dir); err != nil {
		return nil, err
	}
	if err = syncDir(filepath.Dir(dir)); err != nil {
		return nil, err
	}
	return Open(dir, Options{MaxLeaves: capacity})
}

func readImage(r io.Reader, w io.WriterAt, expected [32]byte) (uint64, uint64, [32]byte, error) {
	var h [imageHeader]byte
	var root [32]byte
	fail := func(e error) (uint64, uint64, [32]byte, error) { return 0, 0, root, e }
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return fail(err)
	}
	cap, ver, used := le.Uint64(h[8:]), le.Uint64(h[16:]), le.Uint64(h[24:])
	root = [32]byte(h[32:])
	if string(h[:8]) != imageMagic || cap < 1<<13 || cap > 1<<32 || cap&(cap-1) != 0 || used == 0 || used > cap || used%(SegSize/LeafSize) != 0 {
		return fail(fmt.Errorf("pagerdb: invalid image header"))
	}
	if root != expected {
		return fail(fmt.Errorf("pagerdb: image root differs from expected root"))
	}
	t := newImageTree()
	var idx [8]byte
	var leaf [LeafSize]byte
	seen := false
	for {
		if _, err := io.ReadFull(r, idx[:]); err != nil {
			return fail(err)
		}
		i := le.Uint64(idx[:])
		if i == ^uint64(0) {
			break
		}
		if i >= used || i < t.count || (!seen && i != 0) {
			return fail(fmt.Errorf("pagerdb: invalid image leaf order or offset"))
		}
		if _, err := io.ReadFull(r, leaf[:]); err != nil {
			return fail(err)
		}
		if !seen && (string(leaf[:8]) != magic || le.Uint64(leaf[8:]) != cap || le.Uint64(leaf[hNextSeg:]) != used/(SegSize/LeafSize)) {
			return fail(fmt.Errorf("pagerdb: image state header mismatch"))
		}
		seen = true
		t.zerosTo(i)
		t.add(sha256.Sum256(leaf[:]), 0)
		n, err := w.WriteAt(leaf[:], int64(i*LeafSize))
		if err != nil {
			return fail(err)
		}
		if n != LeafSize {
			return fail(io.ErrShortWrite)
		}
	}
	if !seen {
		return fail(fmt.Errorf("pagerdb: image has no state header"))
	}
	t.zerosTo(cap)
	if t.stack[bits.Len64(cap)-1] != root {
		return fail(fmt.Errorf("pagerdb: image root mismatch"))
	}
	var extra [1]byte
	if n, err := io.ReadFull(r, extra[:]); n != 0 || err != io.EOF {
		return fail(fmt.Errorf("pagerdb: trailing image bytes or missing EOF"))
	}
	if f, ok := w.(interface{ Truncate(int64) error }); ok {
		if err := f.Truncate(int64(used * LeafSize)); err != nil {
			return fail(err)
		}
	}
	return cap, ver, root, nil
}
