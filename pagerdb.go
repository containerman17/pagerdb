// Package pagerdb is a page store: regions of bytes in one mmap'd file, a SHA-256 binary
// tree over 1 KiB leaves, a live log of every write, and sealed epoch files for history.
// It knows pages, versions and roots, nothing about what the bytes mean.
package pagerdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	LeafSize         = 1024    // hash leaf, consensus constant
	SegSize          = 2 << 20 // allocation segment
	DefaultMaxLeaves = 1 << 24 // 16 GiB of state

	nClasses  = 11 // small classes 1 KiB .. 1 MiB
	kindLarge = 12 // region is a chain of 2 MiB segments
	magic     = "PAGERDB1"

	// Header field offsets (state file, segment 0).
	hMagic      = 0
	hMaxLeaves  = 8
	hNextSeg    = 16       // segments in use, header segments included
	hNextRegion = 24       // next never-used region id
	hFreeRegion = 32       // head of the free region id stack
	hFreeSeg    = 40       // head of the free segment chain
	hClasses    = 48       // per class: free slot head u64, carve offset u64
	hNext       = LeafSize // segment chain: u64 per segment
)

var le = binary.LittleEndian

var (
	ErrFull    = errors.New("pagerdb: state capacity exhausted")
	ErrRegion  = errors.New("pagerdb: unknown region")
	ErrPending = errors.New("pagerdb: uncommitted writes pending")
	ErrWindow  = errors.New("pagerdb: version outside the window")
	ErrVersion = errors.New("pagerdb: unknown version or root")
)

// Region is a stable handle to a byte range in the state file.
type Region uint32

// Options configures Open. Zero values pick the defaults.
type Options struct {
	MaxLeaves     uint64     // tree capacity in 1 KiB leaves, power of two, fixed at creation
	Sync          bool       // msync the log at every Commit and state and tree at every checkpoint
	EpochVersions uint64     // close the epoch after this many versions (default 10000)
	EpochBytes    int64      // or after this many log bytes (default 1 GiB)
	Store         EpochStore // sealed epochs (default: local files under dir/epochs)
}

// DB is a page store. One goroutine owns the write API; Views may be used from any goroutine.
type DB struct {
	dir string
	o   Options
	mu  sync.Mutex

	stateF, treeF *os.File
	state         []byte // whole capacity, MAP_SHARED
	tree          []byte // heap-layout nodes, 32 B each, node 1 is the root
	maxLeaves     uint64
	depth         int
	maxSegs       uint64
	maxRegions    uint64
	regTab        uint64 // region table offset
	hdrSegs       uint64
	empty         [][32]byte // hash of an empty subtree per level, 0 = leaf

	version uint64
	root    [32]byte

	dirty    []uint32 // leaves written since the last Commit
	dirtyBit []uint64
	segCache map[Region][]uint64
	maps     map[Region][]byte // contiguous mappings of large regions

	logState
	viewState
	sealState
}

func alignUp(x, a uint64) uint64 { return (x + a - 1) / a * a }

// Open opens or creates a store in dir and recovers it from the live log.
func Open(dir string, o Options) (*DB, error) {
	if o.MaxLeaves == 0 {
		o.MaxLeaves = DefaultMaxLeaves
	}
	if o.MaxLeaves < 1<<12 || o.MaxLeaves&(o.MaxLeaves-1) != 0 || o.MaxLeaves > 1<<32 {
		return nil, fmt.Errorf("pagerdb: MaxLeaves %d must be a power of two in [2^12, 2^32]", o.MaxLeaves)
	}
	if o.EpochVersions == 0 {
		o.EpochVersions = 10000
	}
	if o.EpochBytes == 0 {
		o.EpochBytes = 1 << 30
	}
	for _, d := range []string{dir, filepath.Join(dir, "log"), filepath.Join(dir, "epochs")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	if o.Store == nil {
		s, err := NewLocalStore(filepath.Join(dir, "epochs"))
		if err != nil {
			return nil, err
		}
		o.Store = s
	}
	db := &DB{dir: dir, o: o, maxLeaves: o.MaxLeaves, segCache: map[Region][]uint64{}, maps: map[Region][]byte{}}
	for l := db.maxLeaves; l > 1; l >>= 1 {
		db.depth++
	}
	db.maxSegs = db.maxLeaves * LeafSize / SegSize
	db.maxRegions = db.maxLeaves / 16
	db.regTab = alignUp(hNext+db.maxSegs*8, LeafSize)
	db.hdrSegs = alignUp(db.regTab+db.maxRegions*16, SegSize) / SegSize
	db.dirtyBit = make([]uint64, db.maxLeaves/64)
	db.empty = make([][32]byte, db.depth+1)
	db.empty[0] = sha256Sum(make([]byte, LeafSize))
	for l := 1; l <= db.depth; l++ {
		db.empty[l] = hashPair(db.empty[l-1][:], db.empty[l-1][:])
	}
	if err := db.open(); err != nil {
		db.unmap()
		return nil, err
	}
	return db, nil
}

// mapFiles opens and maps state and tree. fresh truncates them first.
func (db *DB) mapFiles(fresh bool) error {
	flags := os.O_RDWR | os.O_CREATE
	if fresh {
		flags |= os.O_TRUNC
	}
	var err error
	if db.stateF, err = os.OpenFile(filepath.Join(db.dir, "state"), flags, 0o644); err != nil {
		return err
	}
	if db.treeF, err = os.OpenFile(filepath.Join(db.dir, "tree"), flags, 0o644); err != nil {
		return err
	}
	treeSize := int64(2 * db.maxLeaves * 32)
	if err = db.treeF.Truncate(treeSize); err != nil {
		return err
	}
	// The state mapping covers the whole capacity; the file grows under it by segments.
	if db.state, err = unix.Mmap(int(db.stateF.Fd()), 0, int(db.maxLeaves*LeafSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED); err != nil {
		return err
	}
	if db.tree, err = unix.Mmap(int(db.treeF.Fd()), 0, int(treeSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED); err != nil {
		return err
	}
	if fresh {
		if err = db.growState(db.hdrSegs); err != nil {
			return err
		}
		copy(db.state[hMagic:], magic)
		le.PutUint64(db.state[hMaxLeaves:], db.maxLeaves)
		le.PutUint64(db.state[hNextSeg:], db.hdrSegs)
		le.PutUint64(db.state[hNextRegion:], 1)
		db.markDirty(0, hNext)
		return nil
	}
	if string(db.state[hMagic:hMagic+8]) != magic || le.Uint64(db.state[hMaxLeaves:]) != db.maxLeaves {
		return fmt.Errorf("pagerdb: %s: bad magic or MaxLeaves differs from creation", db.dir)
	}
	return nil
}

// growState makes the state file at least segs segments long.
func (db *DB) growState(segs uint64) error {
	fi, err := db.stateF.Stat()
	if err != nil {
		return err
	}
	if uint64(fi.Size()) >= segs*SegSize {
		return nil
	}
	return db.stateF.Truncate(int64(segs * SegSize))
}

func (db *DB) unmap() {
	for r, m := range db.maps {
		unix.Munmap(m)
		delete(db.maps, r)
	}
	for _, c := range db.chunks {
		c.close()
	}
	db.chunks, db.cur = nil, nil
	if db.state != nil {
		unix.Munmap(db.state)
		db.state = nil
	}
	if db.tree != nil {
		unix.Munmap(db.tree)
		db.tree = nil
	}
	if db.stateF != nil {
		db.stateF.Close()
	}
	if db.treeF != nil {
		db.treeF.Close()
	}
}

// Close undoes uncommitted writes, waits for sealing and releases the files.
func (db *DB) Close() error {
	db.stopSealer()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.state == nil {
		return nil
	}
	db.undoTo(db.blockStart, db.blockSeq)
	db.touches = db.touches[:0]
	var err error
	if db.o.Sync {
		err = db.syncAll()
	}
	db.unmap()
	db.closeSealed()
	if err == nil {
		err = db.sealErr
	}
	return err
}

func (db *DB) Version() uint64 { return db.version }
func (db *DB) Root() [32]byte  { return db.root }

func (db *DB) syncAll() error {
	if err := unix.Msync(db.state[:db.hdr(hNextSeg)*SegSize], unix.MS_SYNC); err != nil {
		return err
	}
	if err := unix.Msync(db.tree, unix.MS_SYNC); err != nil {
		return err
	}
	return unix.Msync(db.cur.m[:db.cur.tail], unix.MS_SYNC)
}
