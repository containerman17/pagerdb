package pagerdb

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

var zeros = make([]byte, SegSize)

// classOf returns the small class (1..11) holding size bytes, or 0 above 1 MiB.
func classOf(size uint64) uint32 {
	for c := uint32(1); c <= nClasses; c++ {
		if size <= classSize(c) {
			return c
		}
	}
	return 0
}

func classSize(c uint32) uint64 { return LeafSize << (c - 1) }

func (db *DB) hdr(off uint64) uint64 { return le.Uint64(db.state[off:]) }

func (db *DB) setHdr(off, v uint64) {
	var b [8]byte
	le.PutUint64(b[:], v)
	db.put(0, off, b[:])
}

const regionsPerSegment = SegSize / 16

func regionEntryOff(rd func(uint64) uint64, regTab uint64, r Region) uint64 {
	s := rd(regTab + uint64(r)/regionsPerSegment*8)
	if s == 0 {
		return 0
	}
	return s*SegSize + uint64(r)%regionsPerSegment*16
}

func (db *DB) regionOff(r Region) uint64 {
	return regionEntryOff(db.headU64, db.regTab, r)
}

func (db *DB) setRegion(r Region, kind, n uint32, where uint64) {
	var b [16]byte
	le.PutUint64(b[:], uint64(kind)|uint64(n)<<32)
	le.PutUint64(b[8:], where)
	db.put(0, db.regionOff(r), b[:])
}

// loc is where a region lives: one file range (small class) or a list of segments.
type loc struct {
	size uint64
	off  uint64   // small: file offset
	segs []uint64 // large: segment numbers in region order
}

// locate reads region r's table entry and segment chain through rd (head or a view).
func locate(rd func(off uint64) uint64, regTab, maxRegions uint64, r Region) (loc, error) {
	if r == 0 || uint64(r) >= maxRegions {
		return loc{}, ErrRegion
	}
	off := regionEntryOff(rd, regTab, r)
	if off == 0 {
		return loc{}, ErrRegion
	}
	w0 := rd(off)
	kind, n, where := uint32(w0), uint32(w0>>32), rd(off+8)
	switch {
	case kind >= 1 && kind <= nClasses:
		return loc{size: classSize(kind), off: where}, nil
	case kind == kindLarge:
		l := loc{size: uint64(n) * SegSize, segs: make([]uint64, 0, n)}
		for s := where; len(l.segs) < int(n); s = rd(hNext + s*8) {
			l.segs = append(l.segs, s)
		}
		return l, nil
	}
	return loc{}, ErrRegion
}

// each calls f for every contiguous file range of [off, off+n) in the region.
func (l loc) each(off, n uint64, f func(fileOff, pos, n uint64)) {
	if off+n > l.size || off+n < off {
		panic(fmt.Sprintf("pagerdb: range [%d,%d) outside region of %d bytes", off, off+n, l.size))
	}
	if n == 0 {
		return
	}
	if l.segs == nil {
		f(l.off+off, 0, n)
		return
	}
	for pos := uint64(0); pos < n; {
		o := off + pos
		k := min(n-pos, SegSize-o%SegSize)
		f(l.segs[o/SegSize]*SegSize+o%SegSize, pos, k)
		pos += k
	}
}

func (db *DB) headU64(off uint64) uint64 { return le.Uint64(db.state[off:]) }

// loc resolves a region at head, caching segment chains.
func (db *DB) loc(r Region) loc {
	if segs, ok := db.segCache[r]; ok {
		return loc{size: uint64(len(segs)) * SegSize, segs: segs}
	}
	l, err := locate(db.headU64, db.regTab, db.maxRegions, r)
	if err != nil {
		panic(fmt.Sprintf("pagerdb: region %d: %v", r, err))
	}
	if l.segs != nil {
		db.segCache[r] = l.segs
	}
	return l
}

func (db *DB) forget(r Region) {
	delete(db.segCache, r)
	if m, ok := db.maps[r]; ok {
		unix.Munmap(m)
		delete(db.maps, r)
	}
}

func (db *DB) allocSeg() (uint64, error) {
	if s := db.hdr(hFreeSeg); s != 0 {
		db.setHdr(hFreeSeg, db.hdr(hNext+s*8))
		db.setHdr(hNext+s*8, 0)
		return s, nil
	}
	s := db.hdr(hNextSeg)
	if s >= db.maxSegs {
		return 0, ErrFull
	}
	if err := db.growState(s + 1); err != nil {
		return 0, err
	}
	db.setHdr(hNextSeg, s+1)
	return s, nil
}

// freeSeg zeroes a segment and pushes it on the free chain.
func (db *DB) freeSeg(s uint64) {
	db.put(0, s*SegSize, zeros)
	db.setHdr(hNext+s*8, db.hdr(hFreeSeg))
	db.setHdr(hFreeSeg, s)
}

// allocSegs allocates n segments chained after prev (0 = none) and returns the first.
func (db *DB) allocSegs(n, prev uint64) (uint64, error) {
	first := uint64(0)
	for range n {
		s, err := db.allocSeg()
		if err != nil {
			return 0, err
		}
		if prev != 0 {
			db.setHdr(hNext+prev*8, s)
		}
		if first == 0 {
			first = s
		}
		prev = s
	}
	return first, nil
}

// allocSlot takes a zeroed slot of class c: free list first, then the carve segment, then a new segment.
func (db *DB) allocSlot(c uint32) (uint64, error) {
	h := hClasses + uint64(c-1)*16
	if off := db.hdr(h); off != 0 {
		db.setHdr(h, db.hdr(off))
		db.put(0, off, zeros[:8])
		db.setHdr(hNext+off/SegSize*8, db.hdr(hNext+off/SegSize*8)+1)
		return off, nil
	}
	off := db.hdr(h + 8)
	if off == 0 {
		s, err := db.allocSeg()
		if err != nil {
			return 0, err
		}
		off = s * SegSize
	}
	next := off + classSize(c)
	if next%SegSize == 0 {
		next = 0
	}
	db.setHdr(h+8, next)
	db.setHdr(hNext+off/SegSize*8, db.hdr(hNext+off/SegSize*8)+1)
	return off, nil
}

// freeSlot zeroes a slot and links it into its class free list (link in its first 8 bytes).
// hNext holds the live slot count while a segment belongs to a small class.
func (db *DB) freeSlot(c uint32, off uint64) {
	h := hClasses + uint64(c-1)*16
	db.put(0, off, zeros[:classSize(c)])
	var b [8]byte
	le.PutUint64(b[:], db.hdr(h))
	db.put(0, off, b[:])
	db.setHdr(h, off)
	seg := off / SegSize
	countOff := hNext + seg*8
	count := db.hdr(countOff) - 1
	db.setHdr(countOff, count)
	if count != 0 {
		return
	}
	// Remove this segment's free slots before returning the segment to the pool.
	link := uint64(h)
	for slot := db.hdr(link); slot != 0; slot = db.hdr(link) {
		if slot/SegSize == seg {
			db.setHdr(link, db.hdr(slot))
		} else {
			link = slot
		}
	}
	if db.hdr(h+8)/SegSize == seg {
		db.setHdr(h+8, 0)
	}
	db.freeSeg(seg)
}

func (db *DB) allocID() (Region, error) {
	if r := db.hdr(hFreeRegion); r != 0 {
		db.setHdr(hFreeRegion, db.hdr(db.regionOff(Region(r))+8))
		return Region(r), nil
	}
	r := db.hdr(hNextRegion)
	if r >= db.maxRegions {
		return 0, ErrFull
	}
	directory := db.regTab + r/regionsPerSegment*8
	if db.hdr(directory) == 0 {
		s, err := db.allocSeg()
		if err != nil {
			return 0, err
		}
		db.setHdr(directory, s)
	}
	db.setHdr(hNextRegion, r+1)
	return Region(r), nil
}

// Alloc creates a zeroed region of at least size bytes (rounded up to a class or to 2 MiB segments).
func (db *DB) Alloc(size int) (Region, error) {
	if size <= 0 {
		return 0, fmt.Errorf("pagerdb: Alloc(%d)", size)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	m := db.mark()
	r, err := db.alloc(uint64(size))
	if err != nil {
		db.undo(m)
	}
	return r, err
}

func (db *DB) alloc(size uint64) (Region, error) {
	r, err := db.allocID()
	if err != nil {
		return 0, err
	}
	if c := classOf(size); c != 0 {
		off, err := db.allocSlot(c)
		if err != nil {
			return 0, err
		}
		db.setRegion(r, c, 0, off)
		return r, nil
	}
	n := (size + SegSize - 1) / SegSize
	first, err := db.allocSegs(n, 0)
	if err != nil {
		return 0, err
	}
	db.setRegion(r, kindLarge, uint32(n), first)
	return r, nil
}

// Grow enlarges a region keeping its contents. Small regions move to a bigger class (the
// old slot is freed); large regions get segments appended in place.
func (db *DB) Grow(r Region, size int) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, err := locate(db.headU64, db.regTab, db.maxRegions, r); err != nil {
		return err
	}
	l := db.loc(r)
	if uint64(size) <= l.size {
		return nil
	}
	m := db.mark()
	err := db.grow(r, l, uint64(size))
	if err != nil {
		db.undo(m)
	}
	db.forget(r)
	return err
}

func (db *DB) grow(r Region, l loc, size uint64) error {
	if l.segs != nil {
		n := (size + SegSize - 1) / SegSize
		if _, err := db.allocSegs(n-uint64(len(l.segs)), l.segs[len(l.segs)-1]); err != nil {
			return err
		}
		db.setRegion(r, kindLarge, uint32(n), l.segs[0])
		return nil
	}
	oldC := classOf(l.size)
	if c := classOf(size); c != 0 {
		off, err := db.allocSlot(c)
		if err != nil {
			return err
		}
		db.put(r, off, db.state[l.off:l.off+l.size])
		db.setRegion(r, c, 0, off)
	} else {
		n := (size + SegSize - 1) / SegSize
		first, err := db.allocSegs(n, 0)
		if err != nil {
			return err
		}
		db.put(r, first*SegSize, db.state[l.off:l.off+l.size])
		db.setRegion(r, kindLarge, uint32(n), first)
	}
	db.freeSlot(oldC, l.off)
	return nil
}

// Free zeroes a region, returns its space to the free lists and its id to the id stack.
func (db *DB) Free(r Region) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, err := locate(db.headU64, db.regTab, db.maxRegions, r); err != nil {
		return err
	}
	l := db.loc(r)
	if l.segs == nil {
		db.freeSlot(classOf(l.size), l.off)
	} else {
		for _, s := range l.segs {
			db.freeSeg(s)
		}
	}
	db.setRegion(r, 0, 0, db.hdr(hFreeRegion))
	db.setHdr(hFreeRegion, uint64(r))
	db.forget(r)
	return nil
}

// Map returns the region's head bytes as one writable slice, valid until Free or Grow of r
// (or an Undo or Rewind that reverts its allocation). Writes through it must be announced
// with Touch first. Regions above 2 MiB are mapped back to back with MAP_FIXED.
func (db *DB) Map(r Region) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, err := locate(db.headU64, db.regTab, db.maxRegions, r); err != nil {
		return nil, err
	}
	l := db.loc(r)
	if l.segs == nil {
		return db.state[l.off : l.off+l.size : l.off+l.size], nil
	}
	if m, ok := db.maps[r]; ok {
		return m, nil
	}
	m, err := unix.Mmap(-1, 0, int(l.size), unix.PROT_NONE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	for i, s := range l.segs {
		_, err := unix.MmapPtr(int(db.stateF.Fd()), int64(s*SegSize), unsafe.Pointer(&m[i*SegSize]), SegSize,
			unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_FIXED)
		if err != nil {
			unix.Munmap(m)
			return nil, err
		}
	}
	db.maps[r] = m
	return m, nil
}

// Read copies n head bytes at off into buf, including uncommitted writes.
func (db *DB) Read(r Region, off, n int, buf []byte) {
	db.loc(r).each(uint64(off), uint64(n), func(fo, pos, k uint64) {
		copy(buf[pos:pos+k], db.state[fo:fo+k])
	})
}
