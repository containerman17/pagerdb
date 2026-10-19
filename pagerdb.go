// Package pagerdb stores versioned byte regions with SHA-256 commitments.
// One goroutine owns DB and all mapped slices. Background workers read only
// immutable snapshots. Independent Views may be read concurrently.
package pagerdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

const (
	DefaultMaxLeaves = (16 << 30) / LeafSize // 16 GiB of virtual state space.
	// Allocation metadata reserves one 16-byte entry per possible region.
	headerSize = LeafSize
)

var le = binary.LittleEndian
var (
	ErrFull    = errors.New("pagerdb: state capacity exhausted")
	ErrRegion  = errors.New("pagerdb: unknown region")
	ErrPending = errors.New("pagerdb: uncommitted writes pending")
	ErrWindow  = errors.New("pagerdb: version outside the RAM window")
	ErrVersion = errors.New("pagerdb: unknown version")
	ErrClosed  = errors.New("pagerdb: closed")
)

type Region uint32

// Options fixes state capacity at creation. Other settings can change on Open.
type Options struct {
	MaxLeaves      uint64
	RetainVersions uint64 // Default 32 completed blocks of undo.
	Workers        int    // Default 16 hash and snapshot workers.
	EpochBytes     int64  // Default 1 GiB; a single oversized block stays intact.
}

const snapshotCount = 3

type snapshotSlot struct {
	data []byte
	ids  []uint32
	done sync.WaitGroup
}

type block struct {
	version   uint64
	parent    uint64
	undo      []byte
	ids       []uint32
	data      []byte
	root      [32]byte
	ready     sync.WaitGroup
	persisted sync.WaitGroup
	rewind    bool
	retained  bool
	snapshot  *snapshotSlot
}

// DB methods and mapped slices require a single owner. This includes RootAt,
// Commit, View creation, Rewind and Close. A View owns its bytes after creation.
type DB struct {
	dir          string
	o            Options
	state        []byte
	lock         *os.File
	tree         *tree
	pool         *pool
	version      uint64
	baseVersion  uint64
	baseRoot     [32]byte
	history      []*block
	latest       *block
	undo         []byte
	undoLast     int
	undoHint     int
	dirtyBits    []uint64
	dirtyStart   uint64 // Inclusive first dirty bitmap word.
	dirtyEnd     uint64 // Exclusive last dirty bitmap word.
	generation   uint64
	jobs         chan *block
	hashDone     chan struct{}
	disk         *diskWriter
	closed       bool
	snapshots    [snapshotCount]snapshotSlot
	nextSnapshot int
	blocks       []block
	nextBlock    int
	copying      *snapshotSlot
	copyLeaf     func(int)
	// Test hooks run on background workers, before touching their job.
	beforeHash func()
	beforeDisk func()
	diskHook   atomic.Pointer[func(point string)] // Called at named disk and applier steps.
}

func defaults(o Options) (Options, error) {
	if o.MaxLeaves == 0 {
		o.MaxLeaves = DefaultMaxLeaves
	}
	if o.MaxLeaves < 256 || o.MaxLeaves > 1<<32 || o.MaxLeaves&(o.MaxLeaves-1) != 0 {
		return o, fmt.Errorf("pagerdb: MaxLeaves must be a power of two in [256, 2^32]")
	}
	if o.RetainVersions == 0 {
		o.RetainVersions = 32
	}
	if o.Workers == 0 {
		o.Workers = 16
	}
	if o.Workers < 1 || o.Workers > 256 {
		return o, fmt.Errorf("pagerdb: Workers must be in [1, 256]")
	}
	if o.EpochBytes == 0 {
		o.EpochBytes = 1 << 30
	}
	if o.EpochBytes < 128 {
		return o, fmt.Errorf("pagerdb: EpochBytes must be at least 128")
	}
	return o, nil
}

// Open loads the image file, replays the journal after the image position and
// verifies the recovered root. It returns at the last complete journal frame.
func Open(dir string, o Options) (_ *DB, err error) {
	o, err = defaults(o)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	db := &DB{dir: dir, o: o, generation: 1}
	defer func() {
		if err != nil {
			db.release()
		}
	}()
	db.lock, err = os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(db.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("pagerdb: store already open: %w", err)
	}
	// Reject old formats rather than silently creating a new empty store.
	if _, e := os.Stat(filepath.Join(dir, "state")); e == nil {
		return nil, fmt.Errorf("pagerdb: v2 state requires migration")
	}
	db.state, err = anonymous(int(o.MaxLeaves * LeafSize))
	if err != nil {
		return nil, err
	}
	db.tree, err = newTree(o.MaxLeaves)
	if err != nil {
		return nil, err
	}
	db.pool = newPool(o.Workers)
	db.dirtyBits = make([]uint64, (o.MaxLeaves+63)/64)
	db.dirtyStart = uint64(len(db.dirtyBits))
	db.undo = regrow(nil, 1<<20)
	db.history = make([]*block, 0, o.RetainVersions+1)
	db.blocks = make([]block, o.RetainVersions+snapshotCount+1)
	db.copyLeaf = func(i int) {
		slot := db.copying
		off := uint64(slot.ids[i]) * LeafSize
		copy(slot.data[i*LeafSize:(i+1)*LeafSize], db.state[off:off+LeafSize])
	}
	db.disk, err = openDisk(db)
	if err != nil {
		return nil, err
	}
	db.baseVersion = db.version
	db.baseRoot = db.tree.root()
	db.jobs = make(chan *block, 1)
	db.hashDone = make(chan struct{})
	go db.hashLoop()
	go db.disk.run()
	go db.disk.apply()
	return db, nil
}
func (db *DB) check() {
	if db.closed {
		panic(ErrClosed)
	}
}
func (db *DB) Version() uint64 { db.check(); return db.version }
func (db *DB) markDirty(off, n uint64) {
	if n == 0 {
		return
	}
	for i := off / LeafSize; i <= (off+n-1)/LeafSize; i++ {
		word, mask := i/64, uint64(1)<<(i%64)
		if db.dirtyBits[word]&mask == 0 {
			if db.dirtyBits[word] == 0 {
				db.dirtyStart = min(db.dirtyStart, word)
				db.dirtyEnd = max(db.dirtyEnd, word+1)
			}
			db.dirtyBits[word] |= mask
		}
	}
}

// Undo records: absolute offset u64, old bytes, length u64. The trailing length
// permits reverse traversal without a second index or a per-write allocation.
func (db *DB) touch(off, n uint64) {
	if n == 0 {
		return
	}
	needed := len(db.undo) + 16 + int(n)
	if needed > cap(db.undo) {
		db.undo = regrow(db.undo, max(needed, 2*cap(db.undo), db.undoHint))
	}
	db.undo = le.AppendUint64(db.undo, off)
	db.undo = append(db.undo, db.state[off:off+n]...)
	db.undo = le.AppendUint64(db.undo, n)
	db.markDirty(off, n)
}

// Undo buffers and snapshot slots live in anonymous mappings outside the Go heap,
// so a large live buffer does not raise the GC target and a replaced one is
// returned to the OS at once instead of lingering as idle heap.
func regrow(b []byte, capacity int) []byte {
	grown, err := anonymous(capacity)
	if err != nil {
		panic(err)
	}
	grown = grown[:len(b)]
	copy(grown, b)
	unmap(b)
	return grown
}
func unmap(b []byte) {
	if cap(b) > 0 {
		if err := unix.Munmap(b[:cap(b)]); err != nil {
			panic(err)
		}
	}
}
func restore(state, records []byte, mark func(uint64, uint64)) {
	for end := len(records); end > 0; {
		n := int(le.Uint64(records[end-8:]))
		start := end - 16 - n
		off := le.Uint64(records[start:])
		copy(state[off:off+uint64(n)], records[start+8:end-8])
		if mark != nil {
			mark(off, uint64(n))
		}
		end = start
	}
}

// Mark is valid only in the current block and while its prefix still exists.
type Mark struct {
	generation uint64
	length     int
}

func (db *DB) Mark() Mark { db.check(); return Mark{db.generation, len(db.undo)} }
func (db *DB) Undo(m Mark) {
	db.check()
	if m.generation != db.generation || m.length < 0 || m.length > len(db.undo) {
		panic("pagerdb: invalid mark")
	}
	restore(db.state, db.undo[m.length:], db.markDirty)
	db.undo = db.undo[:m.length]
}
func (db *DB) Touch(r Region, off, n int) {
	db.check()
	p := db.rangeOf(r, off, n)
	db.touch(p, uint64(n))
}
func (db *DB) Write(r Region, off int, data []byte) {
	db.check()
	p := db.rangeOf(r, off, len(data))
	db.touch(p, uint64(len(data)))
	copy(db.state[p:], data)
}
func (db *DB) Read(r Region, off, n int, dst []byte) {
	db.check()
	if len(dst) < n {
		panic("pagerdb: short read buffer")
	}
	p := db.rangeOf(r, off, n)
	copy(dst, db.state[p:p+uint64(n)])
}

// Commit snapshots changed leaves and submits hashing. It waits for the prior
// hash and, if all three snapshot slots are busy, the oldest disk consumer.
// The next block may mutate mapped state as soon as Commit returns.
func (db *DB) Commit() (uint64, error) {
	db.check()
	if db.version == math.MaxUint64 {
		return 0, fmt.Errorf("pagerdb: version exhausted")
	}
	if err := db.disk.failure(); err != nil {
		return 0, err
	}
	db.waitHash()
	b := db.snapshot()
	// Slot acquisition can wait for disk work that reports a new error.
	if err := db.disk.failure(); err != nil {
		b.snapshot.done.Done()
		b.snapshot = nil
		return 0, err
	}
	b.parent = db.version
	db.version++
	b.version = db.version
	// Two consecutive sizes avoid seeding every buffer from one large setup block.
	db.undoHint = min(db.undoLast, len(db.undo))
	db.undoLast = len(db.undo)
	db.undo, b.undo = b.undo[:0], db.undo
	db.generation++
	b.ready.Add(1)
	b.persisted.Add(1)
	b.retained = true
	db.latest = b
	db.history = append(db.history, b)
	if uint64(len(db.history)) > db.o.RetainVersions {
		old := db.history[0]
		old.ready.Wait()
		db.baseVersion = old.version
		db.baseRoot = old.root
		old.retained = false
		copy(db.history, db.history[1:])
		db.history[len(db.history)-1] = nil
		db.history = db.history[:len(db.history)-1]
	}
	db.jobs <- b
	return b.version, nil
}

// Completed blocks remain in a fixed metadata ring while their undo is retained.
// Snapshot storage has a separate, smaller ring and never depends on GC eviction.
func (db *DB) acquireBlock() *block {
	for {
		b := &db.blocks[db.nextBlock]
		db.nextBlock = (db.nextBlock + 1) % len(db.blocks)
		if b.retained {
			continue
		}
		b.ready.Wait()
		b.persisted.Wait()
		b.rewind = false
		return b
	}
}
func (db *DB) snapshot() *block {
	slot := &db.snapshots[db.nextSnapshot]
	db.nextSnapshot = (db.nextSnapshot + 1) % len(db.snapshots)
	slot.done.Wait()
	b := db.acquireBlock()
	slot.ids = db.takeDirty(slot.ids[:0])
	size := len(slot.ids) * LeafSize
	if cap(slot.data) < size {
		// Geometric growth avoids reallocating for each small new high-water mark.
		capacity := min(len(db.state), max(size, 2*cap(slot.data)))
		slot.data = regrow(slot.data[:0], capacity)[:capacity]
	}
	slot.data = slot.data[:size]
	db.copying = slot
	db.pool.each(len(slot.ids), db.copyLeaf)
	db.copying = nil
	b.ids = slot.ids
	b.data = slot.data
	b.snapshot = slot
	slot.done.Add(1)
	return b
}

// takeDirty appends dirty leaf IDs in order to ids and clears the bitmap.
func (db *DB) takeDirty(ids []uint32) []uint32 {
	for word := db.dirtyStart; word < db.dirtyEnd; word++ {
		for pending := db.dirtyBits[word]; pending != 0; pending &= pending - 1 {
			ids = append(ids, uint32(word*64+uint64(bits.TrailingZeros64(pending))))
		}
		db.dirtyBits[word] = 0
	}
	db.dirtyStart = uint64(len(db.dirtyBits))
	db.dirtyEnd = 0
	return ids
}
func (db *DB) waitHash() {
	if db.latest != nil {
		db.latest.ready.Wait()
	}
}
func (db *DB) hashLoop() {
	defer close(db.hashDone)
	for b := range db.jobs {
		if db.beforeHash != nil {
			db.beforeHash()
		}
		db.tree.update(b.ids, b.data, db.pool)
		b.root = db.tree.root()
		db.disk.enqueue(b)
		b.ready.Done()
	}
}

// CommitWait is the blocking compatibility helper.
func (db *DB) CommitWait() ([32]byte, uint64, error) {
	v, err := db.Commit()
	if err != nil {
		return [32]byte{}, 0, err
	}
	r, err := db.RootAt(v)
	return r, v, err
}

// Root returns the committed head root, waiting for hashing if necessary.
func (db *DB) Root() [32]byte {
	r, err := db.RootAt(db.version)
	if err != nil {
		panic(err)
	}
	return r
}
func (db *DB) RootAt(v uint64) ([32]byte, error) {
	db.check()
	if v == db.baseVersion {
		return db.baseRoot, nil
	}
	for i := len(db.history) - 1; i >= 0; i-- {
		b := db.history[i]
		if b.version == v {
			b.ready.Wait()
			return b.root, nil
		}
	}
	if v > db.version {
		return [32]byte{}, ErrVersion
	}
	return [32]byte{}, ErrWindow
}

// Flush waits until queued blocks are written to the journal. It does not fsync
// and does not wait for the image applier.
func (db *DB) Flush() error {
	db.check()
	db.waitHash()
	if db.latest != nil {
		db.latest.persisted.Wait()
	}
	return db.disk.failure()
}

// Checkpoint returns once the journal is synced and the image file and CURRENT
// cover the committed head. The background applier does the same work on its
// own, about once per applyInterval.
func (db *DB) Checkpoint() error {
	db.check()
	if len(db.undo) != 0 {
		return ErrPending
	}
	if err := db.Flush(); err != nil {
		return err
	}
	return db.disk.checkpoint()
}

// Close discards uncommitted writes, drains background work, and releases memory.
// It stops the applier after its current batch; Open replays the rest of the
// journal. Call Checkpoint first to leave a fully applied image.
func (db *DB) Close() error {
	if db.closed {
		return nil
	}
	db.closed = true
	close(db.jobs)
	<-db.hashDone
	db.disk.stop()
	err := db.disk.failure()
	return errors.Join(err, db.release())
}
func (db *DB) release() error {
	// Close can outlive the caller's DB pointer; drop strongly retained buffers.
	for i := range db.snapshots {
		unmap(db.snapshots[i].data)
		db.snapshots[i].data = nil
		db.snapshots[i].ids = nil
	}
	for i := range db.blocks {
		unmap(db.blocks[i].undo)
	}
	unmap(db.undo)
	db.history = nil
	db.blocks = nil
	db.latest = nil
	db.undo = nil
	db.dirtyBits = nil
	if db.disk != nil {
		db.disk.record = nil
		db.disk.frame = nil
	}
	var errs []error
	if db.pool != nil {
		db.pool.close()
		db.pool = nil
	}
	if db.tree != nil {
		errs = append(errs, db.tree.close())
		db.tree = nil
	}
	if db.state != nil {
		errs = append(errs, unix.Munmap(db.state))
		db.state = nil
	}
	if db.lock != nil {
		errs = append(errs, db.lock.Close())
		db.lock = nil
	}
	return errors.Join(errs...)
}
