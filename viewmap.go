package pagerdb

import (
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"unsafe"
)

var errFaultUnavailable = errors.New("pagerdb: userfaultfd unavailable")

type viewMapping struct {
	vw           *View
	loc          loc
	memory, data []byte
	bias         uint64
	closeFault   func()
}

// Map returns a writable private image of region r at the view's version.
// Writes never change the database. The slice is valid until View.Close.
// Serialize Map and Close with the owner's mutation API. Do not access the
// returned slice after Close or concurrently with Close.
func (vw *View) Map(r Region) ([]byte, error) {
	if vw.closed {
		return nil, fmt.Errorf("pagerdb: view closed")
	}
	l, err := vw.loc(r)
	if err != nil {
		return nil, err
	}
	m, err := vw.faultMap(r, l)
	if errors.Is(err, errFaultUnavailable) {
		m, err = vw.privateMap(l)
	}
	if err != nil {
		return nil, err
	}
	vw.db.mu.Lock()
	vw.mappings = append(vw.mappings, m)
	vw.db.mu.Unlock()
	return m.data, nil
}
func (vw *View) privateMap(l loc) (*viewMapping, error) {
	file, err := snapshotState(vw.db)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	m := &viewMapping{vw: vw, loc: l}
	if l.segs == nil {
		page := uint64(unix.Getpagesize())
		off := l.off / page * page
		m.bias = l.off - off
		n := alignUp(m.bias+l.size, page)
		b, err := unix.Mmap(int(file.Fd()), int64(off), int(n), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE)
		if err != nil {
			return nil, err
		}
		m.memory = b
		m.data = b[m.bias : m.bias+l.size : m.bias+l.size]
	} else {
		b, err := unix.Mmap(-1, 0, int(l.size), unix.PROT_NONE, unix.MAP_ANON|unix.MAP_PRIVATE)
		if err != nil {
			return nil, err
		}
		m.memory = b
		m.data = b
		for i, s := range l.segs {
			_, err = unix.MmapPtr(int(file.Fd()), int64(s*SegSize), unsafe.Pointer(&b[i*SegSize]), SegSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_FIXED)
			if err != nil {
				unix.Munmap(b)
				return nil, err
			}
		}
	}
	// Only leaves changed after the requested version need an eager copy.
	changed := make(map[uint64]struct{})
	segments := make(map[uint64]bool, len(l.segs))
	for _, segment := range l.segs {
		segments[segment] = true
	}
	addChanged := func(leaf uint64) {
		off := leaf * LeafSize
		if l.segs == nil {
			if off < l.off || off >= l.off+l.size {
				return
			}
		} else if !segments[off/SegSize] {
			return
		}
		changed[leaf] = struct{}{}
	}
	db := vw.db
	db.mu.Lock()
	for _, e := range db.entries {
		if e.ver > vw.v {
			for leaf := e.off / LeafSize; leaf <= (e.off+uint64(e.n)-1)/LeafSize; leaf++ {
				addChanged(leaf)
			}
		}
	}
	sealed := append([]*sealedEpoch(nil), db.sealed...)
	db.mu.Unlock()
	for _, s := range sealed {
		if s.end > vw.v {
			if err := s.eachChangedLeaf(func(leaf uint64) error { addChanged(leaf); return nil }); err != nil {
				unix.Munmap(m.memory)
				return nil, err
			}
		}
	}
	var readErr error
	l.each(0, l.size, func(fo, pos, n uint64) {
		for p := uint64(0); p < n; p += LeafSize {
			if _, ok := changed[(fo+p)/LeafSize]; ok {
				if err := vw.readFile(fo+p, m.data[pos+p:pos+p+LeafSize]); err != nil {
					readErr = err
					return
				}
			}
		}
	})
	if readErr != nil {
		unix.Munmap(m.memory)
		return nil, readErr
	}
	return m, nil
}

// sparseSnapshot is the fallback when filesystem cloning is unavailable.
func sparseSnapshot(src, dst *os.File) error {
	fi, err := src.Stat()
	if err != nil {
		return err
	}
	if err = dst.Truncate(fi.Size()); err != nil {
		return err
	}
	var buf [64 << 10]byte
	for off := int64(0); off < fi.Size(); {
		data, e := unix.Seek(int(src.Fd()), off, unix.SEEK_DATA)
		if e == unix.ENXIO {
			return nil
		}
		if e != nil {
			data = off
		}
		end := fi.Size()
		if e == nil {
			if hole, er := unix.Seek(int(src.Fd()), data, unix.SEEK_HOLE); er == nil {
				end = hole
			}
		}
		for data < end {
			n, er := src.ReadAt(buf[:min(int64(len(buf)), end-data)], data)
			if er != nil && er != io.EOF {
				return er
			}
			if n == 0 {
				return io.ErrUnexpectedEOF
			}
			if _, er = dst.WriteAt(buf[:n], data); er != nil {
				return er
			}
			data += int64(n)
		}
		off = end
	}
	return nil
}
func (vw *View) closeMaps() {
	for _, m := range vw.mappings {
		if m.closeFault != nil {
			m.closeFault()
		}
		unix.Munmap(m.memory)
	}
	vw.mappings = nil
}
