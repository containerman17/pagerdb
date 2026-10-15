// Package pagerdb stores versioned byte regions with SHA-256 commitments.
// One goroutine owns DB and all mapped slices. Background workers read only
// immutable snapshots. Independent Views may be read concurrently.
package pagerdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	LeafSize         = 4096
	DefaultMaxLeaves = 1 << 22 // 16 GiB of virtual state space.
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
	MaxLeaves          uint64
	RetainVersions     uint64 // Default 32 completed blocks of undo.
	Workers            int    // Default 16 hash and snapshot workers.
	CheckpointInterval uint64 // Default 1000 blocks.
	EpochBytes         int64  // Default 1 GiB; a single oversized block stays intact.
}

type block struct {
	version    uint64
	parent     uint64
	undo       []byte
	ids        []uint32
	data       []byte
	root       [32]byte
	ready      chan struct{}
	persisted  chan struct{}
	checkpoint bool
	rewind     bool
}

// DB methods and mapped slices require a single owner. This includes RootAt,
// Commit, View creation, Rewind and Close. A View owns its bytes after creation.
type DB struct {
	dir         string
	o           Options
	state       []byte
	lock        *os.File
	tree        *tree
	pool        *pool
	version     uint64
	baseVersion uint64
	baseRoot    [32]byte
	history     []*block
	latest      *block
	undo        []byte
	dirty       []uint32
	dirtyBits   []uint64
	generation  uint64
	jobs        chan *block
	hashDone    chan struct{}
	disk        *diskWriter
	closed      bool
	buffers     sync.Pool
	// Test hooks run on background workers, before touching their job.
	beforeHash func()
	beforeDisk func()
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
	if o.CheckpointInterval == 0 {
		o.CheckpointInterval = 1000
	}
	if o.EpochBytes == 0 {
		o.EpochBytes = 1 << 30
	}
	if o.EpochBytes < 128 {
		return o, fmt.Errorf("pagerdb: EpochBytes must be at least 128")
	}
	return o, nil
}

// Open loads and verifies the last published checkpoint. Later journal blocks
// are deliberately not replayed: the caller re-executes them.
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
	db.undo = make([]byte, 0, 1<<20)
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
		mask := uint64(1) << (i % 64)
		if db.dirtyBits[i/64]&mask == 0 {
			db.dirtyBits[i/64] |= mask
			db.dirty = append(db.dirty, uint32(i))
		}
	}
}

// Undo records: absolute offset u64, old bytes, length u64. The trailing length
// permits reverse traversal without a second index or a per-write allocation.
func (db *DB) touch(off, n uint64) {
	if n == 0 {
		return
	}
	db.undo = le.AppendUint64(db.undo, off)
	db.undo = append(db.undo, db.state[off:off+n]...)
	db.undo = le.AppendUint64(db.undo, n)
	db.markDirty(off, n)
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

// Commit snapshots changed leaves and submits hashing. It can wait for the
// previous hash, but never waits for journal or checkpoint I/O. The next block
// may mutate mapped state as soon as Commit returns.
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
	b.parent = db.version
	db.version++
	b.version = db.version
	b.undo = db.undo
	db.undo = nil
	db.generation++
	b.checkpoint = b.version%db.o.CheckpointInterval == 0
	db.latest = b
	db.history = append(db.history, b)
	if uint64(len(db.history)) > db.o.RetainVersions {
		old := db.history[0]
		<-old.ready
		db.baseVersion = old.version
		db.baseRoot = old.root
		// Reuse an expired undo buffer only after the disk worker releases it.
		// A slow disk never makes the owner wait for this optional reuse.
		select {
		case <-old.persisted:
			db.undo = old.undo[:0]
		default:
		}
		db.history[0] = nil
		db.history = db.history[1:]
	}
	if db.undo == nil {
		db.undo = make([]byte, 0, max(1<<20, len(b.undo)))
	}
	db.jobs <- b
	return b.version, nil
}
func (db *DB) snapshot() *block {
	slices.Sort(db.dirty)
	ids := db.dirty
	db.dirty = nil
	size := len(ids) * LeafSize
	var data []byte
	if old := db.buffers.Get(); old != nil {
		data = old.([]byte)
	}
	if cap(data) < size {
		data = make([]byte, size)
	} else {
		data = data[:size]
	}
	db.pool.each(len(ids), func(i int) {
		off := uint64(ids[i]) * LeafSize
		copy(data[i*LeafSize:(i+1)*LeafSize], db.state[off:off+LeafSize])
	})
	for _, id := range ids {
		db.dirtyBits[id/64] &= ^(uint64(1) << (id % 64))
	}
	return &block{ids: ids, data: data, ready: make(chan struct{}), persisted: make(chan struct{})}
}
func (db *DB) waitHash() {
	if db.latest != nil {
		<-db.latest.ready
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
		close(b.ready)
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
			<-b.ready
			return b.root, nil
		}
	}
	if v > db.version {
		return [32]byte{}, ErrVersion
	}
	return [32]byte{}, ErrWindow
}

// Flush waits for queued disk work. It does not force a checkpoint or fsync.
func (db *DB) Flush() error {
	db.check()
	db.waitHash()
	if db.latest != nil {
		<-db.latest.persisted
	}
	return db.disk.failure()
}

// Checkpoint publishes the current committed version durably. Ordinary commits
// schedule this automatically every CheckpointInterval blocks.
func (db *DB) Checkpoint() error {
	db.check()
	if len(db.undo) != 0 {
		return ErrPending
	}
	db.waitHash()
	b := &block{version: db.version, root: db.Root(), checkpoint: true, persisted: make(chan struct{})}
	db.disk.enqueue(b)
	<-b.persisted
	return db.disk.failure()
}

// Close discards uncommitted writes, drains background work, and releases memory.
// It does not add a checkpoint. Call Checkpoint explicitly when required.
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
	var errs []error
	if db.pool != nil {
		db.pool.close()
	}
	if db.tree != nil {
		errs = append(errs, db.tree.close())
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
