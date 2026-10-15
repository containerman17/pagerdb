package pagerdb

import (
	"bytes"
	"fmt"
	"slices"
)

func align(x uint64) uint64       { return (x + LeafSize - 1) / LeafSize * LeafSize }
func regionLimit(n uint64) uint64 { return min(n, 1<<20) }
func dataStart(n uint64) uint64   { return align(headerSize + regionLimit(n)*16) }
func entry(r Region) uint64       { return headerSize + uint64(r)*16 }
func location(state []byte, r Region) (off, size uint64, err error) {
	n := le.Uint64(state[8:])
	limit := regionLimit(n)
	if r == 0 || uint64(r) >= limit {
		return 0, 0, ErrRegion
	}
	p := entry(r)
	off = le.Uint64(state[p:])
	size = le.Uint64(state[p+8:])
	if size == 0 {
		return 0, 0, ErrRegion
	}
	return
}
func (db *DB) rangeOf(r Region, off, n int) uint64 {
	p, size, err := location(db.state, r)
	if err != nil {
		panic(err)
	}
	if off < 0 || n < 0 || uint64(off) > size || uint64(n) > size-uint64(off) {
		panic("pagerdb: range outside region")
	}
	return p + uint64(off)
}
func (db *DB) setEntry(r Region, off, size uint64) {
	p := entry(r)
	db.touch(p, 16)
	le.PutUint64(db.state[p:], off)
	le.PutUint64(db.state[p+8:], size)
}

// Regions use contiguous, leaf-aligned first-fit extents. Handles are never
// reused, so a stale handle cannot name a later allocation.
func (db *DB) space(size uint64, exclude Region) (uint64, error) {
	type extent struct{ off, end uint64 }
	var used []extent
	for r := Region(1); uint64(r) < le.Uint64(db.state[16:]); r++ {
		if r == exclude {
			continue
		}
		off, n, err := location(db.state, r)
		if err == nil {
			used = append(used, extent{off, off + align(n)})
		}
	}
	slices.SortFunc(used, func(a, b extent) int {
		if a.off < b.off {
			return -1
		}
		if a.off > b.off {
			return 1
		}
		return 0
	})
	pos := dataStart(db.o.MaxLeaves)
	need := align(size)
	for _, e := range used {
		if e.off-pos >= need {
			return pos, nil
		}
		pos = e.end
	}
	if pos > uint64(len(db.state)) || need > uint64(len(db.state))-pos {
		return 0, ErrFull
	}
	return pos, nil
}
func (db *DB) zero(off, n uint64) {
	var zero [LeafSize]byte
	for end := off + n; off < end; {
		size := min(uint64(LeafSize), end-off)
		if !bytes.Equal(db.state[off:off+size], zero[:size]) {
			db.touch(off, size)
			clear(db.state[off : off+size])
		}
		off += size
	}
}
func (db *DB) Alloc(size int) (Region, error) {
	db.check()
	if size <= 0 {
		return 0, fmt.Errorf("pagerdb: allocation size must be positive")
	}
	next := le.Uint64(db.state[16:])
	if next >= regionLimit(db.o.MaxLeaves) {
		return 0, ErrFull
	}
	off, err := db.space(uint64(size), 0)
	if err != nil {
		return 0, err
	}
	db.touch(16, 8)
	le.PutUint64(db.state[16:], next+1)
	db.zero(off, align(uint64(size)))
	r := Region(next)
	db.setEntry(r, off, uint64(size))
	return r, nil
}

// Grow may relocate a region. All previous Map slices for it become invalid.
func (db *DB) Grow(r Region, size int) error {
	db.check()
	off, old, err := location(db.state, r)
	if err != nil {
		return err
	}
	if size < 0 || uint64(size) < old {
		return fmt.Errorf("pagerdb: Grow cannot shrink")
	}
	if uint64(size) == old {
		return nil
	}
	// Keep the existing address when the following extent has enough space.
	end := uint64(len(db.state))
	for id := Region(1); uint64(id) < le.Uint64(db.state[16:]); id++ {
		p, _, e := location(db.state, id)
		if e == nil && id != r && p > off {
			end = min(end, p)
		}
	}
	if align(uint64(size)) <= end-off {
		db.zero(off+old, align(uint64(size))-old)
		db.setEntry(r, off, uint64(size))
		return nil
	}
	dest, err := db.space(uint64(size), r)
	if err != nil {
		return err
	}
	saved := bytes.Clone(db.state[off : off+old])
	db.zero(off, align(old))
	db.zero(dest, align(uint64(size)))
	db.touch(dest, old)
	copy(db.state[dest:], saved)
	db.setEntry(r, dest, uint64(size))
	return nil
}

// Free invalidates all Map slices for r and clears its bytes.
func (db *DB) Free(r Region) error {
	db.check()
	off, size, err := location(db.state, r)
	if err != nil {
		return err
	}
	db.zero(off, align(size))
	db.setEntry(r, 0, 0)
	return nil
}

// Map returns zero-copy anonymous memory. Touch must precede each direct write.
// Grow, Free, Undo of allocation changes, Rewind and Close invalidate the slice.
func (db *DB) Map(r Region) ([]byte, error) {
	db.check()
	off, size, err := location(db.state, r)
	if err != nil {
		return nil, err
	}
	return db.state[off : off+size : off+size], nil
}
