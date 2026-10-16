package pagerdb

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

const (
	frameHeader   = 72
	manifestBytes = 80
	// maxApplyLag bounds the journal bytes the image file may trail. Past it the
	// disk worker stops appending, the three snapshot slots fill and Commit waits.
	maxApplyLag = 4 << 30
)

// applyInterval lets one image write cover a leaf rewritten by many blocks.
// Crash tests shorten it before Open.
var applyInterval = time.Second

var (
	errTorn   = errors.New("pagerdb: damaged journal frame")
	errRecord = errors.New("pagerdb: invalid journal record")
)

type position struct {
	epoch  uint64
	offset int64
}

type diskWriter struct {
	db          *DB
	journal     *os.File
	record      []byte
	masks       [][2]uint64
	index       []uint32 // leaf id -> position in the current block's ids
	queue       [snapshotCount + 1]*block
	queueHead   int
	queueLength int
	done        chan struct{}
	// mu guards the queue, the journal end, the byte counters and the stop flags.
	mu             sync.Mutex
	cond           *sync.Cond
	epoch          uint64
	journalBytes   int64
	written        int64 // journal bytes after the image position at Open, plus appends
	applied        int64 // the part of written that the published image covers
	appliedVersion uint64
	waiters        int // Checkpoint or a lag-bound append wants the applier now
	stopping       bool
	applyStop      bool
	err            error
	lagLimit       int64
	// The applier goroutine owns these after Open.
	image     *os.File
	mapped    []byte // MAP_SHARED image file
	pos       position
	oldest    uint64
	reader    *os.File
	frame     []byte
	next      time.Time
	timer     *time.Timer
	applyDone chan struct{}
}

func (d *diskWriter) journalPath(epoch uint64) string {
	return filepath.Join(d.db.dir, "log", fmt.Sprintf("%020d.journal", epoch))
}
func (d *diskWriter) hook(point string) {
	if h := d.db.diskHook.Load(); h != nil {
		(*h)(point)
	}
}

func openDisk(db *DB) (_ *diskWriter, err error) {
	d := &diskWriter{db: db, done: make(chan struct{}), applyDone: make(chan struct{}), lagLimit: maxApplyLag}
	d.cond = sync.NewCond(&d.mu)
	defer func() {
		if err != nil {
			d.closeFiles()
		}
	}()
	logDir := filepath.Join(db.dir, "log")
	manifest, err := os.ReadFile(filepath.Join(db.dir, "CURRENT"))
	fresh := errors.Is(err, os.ErrNotExist)
	if err != nil && !fresh {
		return nil, err
	}
	if !fresh && (len(manifest) != manifestBytes || string(manifest[:8]) != "PGIMG004" || crc32.Checksum(manifest[:76], crcTable) != le.Uint32(manifest[76:])) {
		return nil, fmt.Errorf("pagerdb: invalid image manifest")
	}
	if fresh {
		entries, e := os.ReadDir(logDir)
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			return nil, e
		}
		if len(entries) != 0 {
			return nil, fmt.Errorf("pagerdb: missing image manifest in an existing store")
		}
	}
	if d.image, err = os.OpenFile(filepath.Join(db.dir, "image"), os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return nil, err
	}
	if fresh {
		// An image without CURRENT was never published. Start from a sparse zero file.
		if err = d.image.Truncate(0); err == nil {
			err = d.image.Truncate(int64(len(db.state)))
		}
		if err != nil {
			return nil, err
		}
	}
	st, err := d.image.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() != int64(len(db.state)) {
		return nil, fmt.Errorf("pagerdb: image size differs from capacity")
	}
	if d.mapped, err = unix.Mmap(int(d.image.Fd()), 0, len(db.state), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}
	if fresh {
		copy(db.state, "PAGER003")
		le.PutUint64(db.state[8:], db.o.MaxLeaves)
		le.PutUint64(db.state[16:], 1)
		copy(d.mapped, db.state[:LeafSize])
		db.markDirty(0, LeafSize)
		db.hashDirty()
		d.pos, d.oldest = position{1, 0}, 1
		err = d.publish(0, db.tree.root())
	} else {
		err = d.recover(manifest)
	}
	if err != nil {
		return nil, err
	}
	if err = d.rotate(); err != nil {
		return nil, err
	}
	return d, nil
}

// recover loads the image, replays every journal frame after its position and
// checks the final root. The first damaged frame ends the chain: the applier
// never used it, so it and every later epoch are removed before new appends.
func (d *diskWriter) recover(m []byte) error {
	db := d.db
	if le.Uint64(m[8:]) != db.o.MaxLeaves {
		return fmt.Errorf("pagerdb: capacity differs from the store")
	}
	d.pos = position{le.Uint64(m[16:]), int64(le.Uint64(m[24:]))}
	version, root := le.Uint64(m[32:]), [32]byte(m[40:72])
	if d.pos.epoch == 0 || d.pos.offset < 0 {
		return fmt.Errorf("pagerdb: invalid image position")
	}
	d.appliedVersion, d.oldest = version, d.pos.epoch
	if err := d.load(); err != nil {
		return err
	}
	e, off := d.pos.epoch, d.pos.offset
	for ; ; e, off = e+1, 0 {
		f, err := os.OpenFile(d.journalPath(e), os.O_RDWR, 0)
		if errors.Is(err, os.ErrNotExist) {
			if off != 0 {
				return fmt.Errorf("pagerdb: journal at the image position is missing")
			}
			break
		}
		if err != nil {
			return err
		}
		end, torn, err := d.replay(f, off, &version, &root)
		if err == nil && torn {
			err = errors.Join(f.Truncate(end), f.Sync())
		}
		if err = errors.Join(err, f.Close()); err != nil {
			return err
		}
		if torn {
			e++
			break
		}
	}
	entries, err := os.ReadDir(filepath.Join(db.dir, "log"))
	if err != nil {
		return err
	}
	removed := false
	for _, ent := range entries {
		var n uint64
		if _, err := fmt.Sscanf(ent.Name(), "%020d.journal", &n); err == nil && (n < d.pos.epoch || n >= e) {
			if err = os.Remove(filepath.Join(db.dir, "log", ent.Name())); err != nil {
				return err
			}
			removed = true
		}
	}
	// New epochs reuse removed names, so the removals must be durable first.
	if removed {
		if err = syncDir(filepath.Join(db.dir, "log")); err != nil {
			return err
		}
	}
	d.epoch = e - 1
	db.hashDirty()
	if db.tree.root() != root {
		return fmt.Errorf("pagerdb: recovered root mismatch")
	}
	if err = validateAlloc(db.state, db.o.MaxLeaves); err != nil {
		return err
	}
	db.version = version
	return nil
}

// load copies every nonzero image leaf into state. SEEK_DATA skips sparse holes.
func (d *diskWriter) load() error {
	fd, size := int(d.image.Fd()), int64(len(d.mapped))
	var zero [LeafSize]byte
	for off := int64(0); off < size; {
		start, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			break
		}
		end := size
		if err != nil {
			start = off // No SEEK_DATA support: scan the rest.
		} else if end, err = unix.Seek(fd, start, unix.SEEK_HOLE); err != nil {
			return err
		}
		start -= start % LeafSize
		end = min(size, int64(align(uint64(end))))
		for p := start; p < end; p += LeafSize {
			if leaf := d.mapped[p : p+LeafSize]; !bytes.Equal(leaf, zero[:]) {
				copy(d.db.state[p:], leaf)
				d.db.markDirty(uint64(p), LeafSize)
			}
		}
		off = end
	}
	return nil
}

// replay applies one epoch's frames from off to state. It returns the end of the
// valid frames and whether a damaged frame follows them.
func (d *diskWriter) replay(f *os.File, off int64, version *uint64, root *[32]byte) (int64, bool, error) {
	st, err := f.Stat()
	if err != nil {
		return off, false, err
	}
	if off > st.Size() {
		return off, false, fmt.Errorf("pagerdb: journal shorter than the image position")
	}
	db := d.db
	for off < st.Size() {
		frame, err := d.readFrame(f, off, st.Size())
		if errors.Is(err, errTorn) {
			return off, true, nil
		}
		if err != nil {
			return off, false, err
		}
		if le.Uint64(frame[24:]) != *version {
			return off, false, fmt.Errorf("pagerdb: journal parent mismatch")
		}
		err = walk(redoOf(frame), uint64(len(db.state)), func(p uint64, b []byte) {
			copy(db.state[p:], b)
			db.markDirty(p, uint64(len(b)))
		})
		if err != nil {
			return off, false, err
		}
		*version, *root = le.Uint64(frame[16:]), [32]byte(frame[32:64])
		d.written += int64(len(frame))
		off += int64(len(frame))
	}
	return off, false, nil
}

// Frame: magic "PGJNL004", frame bytes, version, parent version, root, flags
// (1 = rewind), redo records, CRC-32C of all before it.
func (d *diskWriter) readFrame(f *os.File, off, end int64) ([]byte, error) {
	if end-off < frameHeader+4 {
		return nil, errTorn
	}
	b := slices.Grow(d.frame[:0], frameHeader)[:frameHeader]
	if _, err := f.ReadAt(b, off); err != nil {
		return nil, err
	}
	n := le.Uint64(b[8:])
	if string(b[:8]) != "PGJNL004" || n > uint64(end-off) || n < frameHeader+4 || le.Uint64(b[64:]) > 1 {
		return nil, errTorn
	}
	b = slices.Grow(b, int(n)-frameHeader)[:n]
	d.frame = b
	if _, err := f.ReadAt(b[frameHeader:], off+frameHeader); err != nil {
		return nil, err
	}
	if crc32.Checksum(b[:n-4], crcTable) != le.Uint32(b[n-4:]) {
		return nil, errTorn
	}
	return b, nil
}
func redoOf(frame []byte) []byte { return frame[frameHeader : len(frame)-4] }

// chunk is the redo granularity: a leaf has up to 128 chunks, one mask bit each.
const (
	chunk  = 32
	chunks = LeafSize / chunk
)

// fullMask marks every chunk of a leaf.
var fullMask = func() (m [2]uint64) {
	for c := range chunks {
		m[c/64] |= 1 << (c % 64)
	}
	return
}()

// walk visits redo records (leaf id u64, two u64 chunk masks, the set chunks'
// bytes), one call per run of set chunks, checking bounds before each visit.
func walk(records []byte, limit uint64, fn func(off uint64, b []byte)) error {
	for len(records) > 0 {
		if len(records) < 24 {
			return errRecord
		}
		leaf, m := le.Uint64(records), [2]uint64{le.Uint64(records[8:]), le.Uint64(records[16:])}
		records = records[24:]
		n := chunk * (bits.OnesCount64(m[0]) + bits.OnesCount64(m[1]))
		if leaf >= limit/LeafSize || n == 0 || n > len(records) || m[0]&^fullMask[0] != 0 || m[1]&^fullMask[1] != 0 {
			return errRecord
		}
		set := func(c int) bool { return c < chunks && m[c/64]>>(c%64)&1 == 1 }
		for c := 0; c < chunks; c++ {
			if !set(c) {
				continue
			}
			start := c
			for set(c + 1) {
				c++
			}
			k := (c + 1 - start) * chunk
			fn(leaf*LeafSize+uint64(start*chunk), records[:k])
			records = records[k:]
		}
	}
	return nil
}

// publish makes the image durable, then names it in CURRENT with an atomic rename.
func (d *diskWriter) publish(version uint64, root [32]byte) error {
	if err := syncImage(d.mapped, int(d.image.Fd())); err != nil {
		return err
	}
	d.hook("image-synced")
	m := make([]byte, manifestBytes)
	copy(m, "PGIMG004")
	le.PutUint64(m[8:], d.db.o.MaxLeaves)
	le.PutUint64(m[16:], d.pos.epoch)
	le.PutUint64(m[24:], uint64(d.pos.offset))
	le.PutUint64(m[32:], version)
	copy(m[40:], root[:])
	le.PutUint32(m[76:], crc32.Checksum(m[:76], crcTable))
	path := filepath.Join(d.db.dir, "CURRENT.tmp")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(m)
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	d.hook("manifest-written")
	if err = os.Rename(path, filepath.Join(d.db.dir, "CURRENT")); err != nil {
		return err
	}
	d.hook("manifest-renamed")
	return syncDir(d.db.dir)
}
func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

// apply runs the background applier. It syncs the journal, copies the redo of
// the synced frames into the image file, syncs the image and publishes CURRENT.
// It starts a batch at most once per applyInterval unless someone waits for it.
func (d *diskWriter) apply() {
	defer close(d.applyDone)
	debug.SetPanicOnFault(true) // An image I/O error arrives as a fault on the mapping.
	d.timer = time.AfterFunc(time.Hour, func() { d.mu.Lock(); d.cond.Broadcast(); d.mu.Unlock() })
	for {
		d.mu.Lock()
		for {
			if d.applyStop || d.err != nil {
				d.mu.Unlock()
				return
			}
			work := d.pos != position{d.epoch, d.journalBytes}
			if work && (d.waiters > 0 || !time.Now().Before(d.next)) {
				break
			}
			if work {
				d.timer.Reset(time.Until(d.next))
			}
			d.cond.Wait()
		}
		epoch, end := d.epoch, d.journalBytes
		d.mu.Unlock()
		if err := d.batch(epoch, end); err != nil {
			d.mu.Lock()
			d.err = errors.Join(d.err, err)
			d.cond.Broadcast()
			d.mu.Unlock()
			return
		}
	}
}
func (d *diskWriter) batch(epoch uint64, end int64) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pagerdb: image applier: %v", r)
		}
	}()
	d.hook("apply")
	var version uint64
	var root [32]byte
	var applied int64
	for {
		if d.reader == nil {
			if d.reader, err = os.Open(d.journalPath(d.pos.epoch)); err != nil {
				return err
			}
			// A new epoch's directory entry must be durable before its frames are applied.
			if err = syncDir(filepath.Join(d.db.dir, "log")); err != nil {
				return err
			}
		}
		limit := end
		if d.pos.epoch < epoch {
			st, err := d.reader.Stat()
			if err != nil {
				return err
			}
			limit = st.Size()
		}
		if d.pos.offset < limit {
			// Only frames covered by this sync may reach the image.
			if err = dataSync(int(d.reader.Fd())); err != nil {
				return err
			}
		}
		for d.pos.offset < limit {
			frame, err := d.readFrame(d.reader, d.pos.offset, limit)
			if err != nil {
				return err
			}
			if err = walk(redoOf(frame), uint64(len(d.mapped)), func(p uint64, b []byte) { copy(d.mapped[p:], b) }); err != nil {
				return err
			}
			version, root = le.Uint64(frame[16:]), [32]byte(frame[32:64])
			d.pos.offset += int64(len(frame))
			applied += int64(len(frame))
			d.hook("applying")
		}
		if d.pos.epoch == epoch {
			break
		}
		err = d.reader.Close()
		d.reader = nil
		d.pos = position{d.pos.epoch + 1, 0}
		if err != nil {
			return err
		}
	}
	if applied == 0 {
		return nil
	}
	d.next = time.Now().Add(applyInterval)
	if err = d.publish(version, root); err != nil {
		return err
	}
	for ; d.oldest < d.pos.epoch; d.oldest++ {
		if err = os.Remove(d.journalPath(d.oldest)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	d.mu.Lock()
	d.applied += applied
	d.appliedVersion = version
	d.cond.Broadcast()
	d.mu.Unlock()
	return nil
}

// checkpoint waits until the published image covers every appended frame.
func (d *diskWriter) checkpoint() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	target := d.written
	d.waiters++
	d.cond.Broadcast()
	for d.applied < target && d.err == nil {
		d.cond.Wait()
	}
	d.waiters--
	return d.err
}

// applyState reports the published image version and the unapplied journal bytes.
func (d *diskWriter) applyState() (uint64, int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.appliedVersion, d.written - d.applied
}

func (d *diskWriter) rotate() error {
	if d.journal != nil {
		if err := d.journal.Close(); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(d.journalPath(d.epoch+1), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	d.journal = f
	d.mu.Lock()
	d.epoch++
	d.journalBytes = 0
	d.cond.Broadcast()
	d.mu.Unlock()
	d.hook("rotated")
	return nil
}

// final copies the block's committed bytes at absolute offset off into dst.
// mark sets the mask bit of every chunk in [off, off+n). index maps each of the
// block's leaves to its position in b.ids; entries for other leaves are stale.
func (b *block) mark(masks [][2]uint64, index []uint32, off, n uint64) {
	for end := off + n; off < end; {
		leaf := off / LeafSize
		i := index[leaf]
		if int(i) >= len(b.ids) || b.ids[i] != uint32(leaf) {
			panic("pagerdb: undo range outside the snapshot")
		}
		last := min(end, (leaf+1)*LeafSize) - 1
		for c := off % LeafSize / chunk; c <= last%LeafSize/chunk; c++ {
			masks[i][c/64] |= 1 << (c % 64)
		}
		off = last + 1
	}
}
func (d *diskWriter) append(b *block) error {
	d.mu.Lock()
	if d.written-d.applied > d.lagLimit {
		d.waiters++
		d.cond.Broadcast()
		for d.written-d.applied > d.lagLimit && d.err == nil {
			d.cond.Wait()
		}
		d.waiters--
	}
	err := d.err
	d.mu.Unlock()
	if err != nil {
		return err
	}
	masks := slices.Grow(d.masks[:0], len(b.ids))[:len(b.ids)]
	d.masks = masks
	if b.rewind {
		// A rewind's redo is every leaf it changed.
		for i := range masks {
			masks[i] = fullMask
		}
	} else {
		clear(masks)
		if d.index == nil {
			d.index = make([]uint32, d.db.o.MaxLeaves)
		}
		for i, id := range b.ids {
			d.index[id] = uint32(i)
		}
		for u := b.undo; len(u) > 0; {
			n := le.Uint64(u[len(u)-8:])
			start := len(u) - 16 - int(n)
			b.mark(masks, d.index, le.Uint64(u[start:]), n)
			u = u[:start]
		}
	}
	size := frameHeader + 4 + len(b.ids)*24 + min(len(b.undo), len(b.data))
	record := slices.Grow(d.record[:0], size)[:frameHeader]
	clear(record)
	copy(record, "PGJNL004")
	le.PutUint64(record[16:], b.version)
	le.PutUint64(record[24:], b.parent)
	copy(record[32:64], b.root[:])
	if b.rewind {
		le.PutUint64(record[64:], 1)
	}
	// Leaf order reads the snapshot forward; random undo order missed the cache.
	for i, m := range masks {
		if m == [2]uint64{} {
			continue
		}
		record = le.AppendUint64(record, uint64(b.ids[i]))
		record = le.AppendUint64(record, m[0])
		record = le.AppendUint64(record, m[1])
		leaf := b.data[i*LeafSize : (i+1)*LeafSize]
		for w, x := range m {
			for ; x != 0; x &= x - 1 {
				c := w*64 + bits.TrailingZeros64(x)
				record = append(record, leaf[c*chunk:(c+1)*chunk]...)
			}
		}
	}
	le.PutUint64(record[8:], uint64(len(record)+4))
	record = le.AppendUint32(record, crc32.Checksum(record, crcTable))
	d.record = record
	if d.journalBytes > 0 && d.journalBytes+int64(len(record)) > d.db.o.EpochBytes {
		if err := d.rotate(); err != nil {
			return err
		}
	}
	n, err := d.journal.Write(record)
	d.mu.Lock()
	d.journalBytes += int64(n)
	d.written += int64(n)
	d.cond.Broadcast()
	d.mu.Unlock()
	if err == nil && n != len(record) {
		err = io.ErrShortWrite
	}
	return err
}
func (d *diskWriter) enqueue(b *block) {
	d.mu.Lock()
	if d.queueLength == len(d.queue) {
		panic("pagerdb: disk queue exceeded snapshot ring")
	}
	d.queue[(d.queueHead+d.queueLength)%len(d.queue)] = b
	d.queueLength++
	d.cond.Broadcast()
	d.mu.Unlock()
}
func (d *diskWriter) failure() error { d.mu.Lock(); defer d.mu.Unlock(); return d.err }
func (d *diskWriter) run() {
	defer close(d.done)
	for {
		d.mu.Lock()
		for d.queueLength == 0 && !d.stopping {
			d.cond.Wait()
		}
		if d.queueLength == 0 && d.stopping {
			d.mu.Unlock()
			break
		}
		b := d.queue[d.queueHead]
		d.queue[d.queueHead] = nil
		d.queueHead = (d.queueHead + 1) % len(d.queue)
		d.queueLength--
		failed := d.err != nil
		d.mu.Unlock()
		var err error
		if !failed {
			if d.db.beforeDisk != nil {
				d.db.beforeDisk()
			}
			err = d.append(b)
		}
		b.data = nil
		b.ids = nil
		slot := b.snapshot
		b.snapshot = nil
		if err != nil {
			d.mu.Lock()
			d.err = errors.Join(d.err, err)
			d.cond.Broadcast()
			d.mu.Unlock()
		}
		if slot != nil {
			slot.done.Done()
		}
		b.persisted.Done()
	}
}

// stop drains the disk queue, then stops the applier after its current batch.
// Frames it has not applied stay in the journal and replay on the next Open.
func (d *diskWriter) stop() {
	d.mu.Lock()
	d.stopping = true
	d.cond.Broadcast()
	d.mu.Unlock()
	<-d.done
	d.mu.Lock()
	d.applyStop = true
	d.cond.Broadcast()
	d.mu.Unlock()
	<-d.applyDone
	d.timer.Stop()
	err := d.closeFiles()
	d.mu.Lock()
	d.err = errors.Join(d.err, err)
	d.mu.Unlock()
}
func (d *diskWriter) closeFiles() error {
	var errs []error
	for _, f := range []*os.File{d.journal, d.reader, d.image} {
		if f != nil {
			errs = append(errs, f.Close())
		}
	}
	if d.mapped != nil {
		errs = append(errs, unix.Munmap(d.mapped))
	}
	d.journal, d.reader, d.image, d.mapped = nil, nil, nil, nil
	return errors.Join(errs...)
}

func validateAlloc(state []byte, n uint64) error {
	if string(state[:8]) != "PAGER003" || le.Uint64(state[8:]) != n {
		return fmt.Errorf("pagerdb: invalid state header")
	}
	next := le.Uint64(state[16:])
	if next < 1 || next > regionLimit(n) {
		return fmt.Errorf("pagerdb: invalid region count")
	}
	type span struct{ off, end uint64 }
	var spans []span
	for r := uint64(1); r < next; r++ {
		off, size, err := location(state, Region(r))
		if err != nil {
			continue
		}
		if off < dataStart(n) || off%LeafSize != 0 || off > uint64(len(state)) || size > uint64(len(state))-off {
			return fmt.Errorf("pagerdb: invalid region extent")
		}
		spans = append(spans, span{off, off + align(size)})
	}
	slices.SortFunc(spans, func(a, b span) int {
		if a.off < b.off {
			return -1
		}
		if a.off > b.off {
			return 1
		}
		return 0
	})
	for i := 1; i < len(spans); i++ {
		if spans[i].off < spans[i-1].end {
			return fmt.Errorf("pagerdb: overlapping regions")
		}
	}
	return nil
}

// rebuild computes the root without consulting dirty tracking or the live tree.
func (db *DB) rebuild() ([32]byte, error) {
	t, err := newTree(db.o.MaxLeaves)
	if err != nil {
		return [32]byte{}, err
	}
	defer t.close()
	// Hash the metadata and all allocated extents. Freed extents are always zero.
	var ids []uint32
	for off := uint64(0); off < dataStart(db.o.MaxLeaves); off += LeafSize {
		ids = append(ids, uint32(off/LeafSize))
	}
	for r := Region(1); uint64(r) < le.Uint64(db.state[16:]); r++ {
		off, size, e := location(db.state, r)
		if e == nil {
			for p := off; p < off+align(size); p += LeafSize {
				ids = append(ids, uint32(p/LeafSize))
			}
		}
	}
	slices.Sort(ids)
	hashLeaves(t, db.state, ids, db.pool)
	return t.root(), nil
}

// hashLeaves hashes sorted state leaves into t, bounding scratch memory.
func hashLeaves(t *tree, state []byte, ids []uint32, p *pool) {
	data := make([]byte, min(len(ids), 1024)*LeafSize)
	for len(ids) > 0 {
		n := min(len(ids), 1024)
		for i, id := range ids[:n] {
			copy(data[i*LeafSize:(i+1)*LeafSize], state[uint64(id)*LeafSize:])
		}
		t.update(ids[:n], data[:n*LeafSize], p)
		ids = ids[n:]
	}
}

// hashDirty hashes every dirty leaf into the live tree and clears the bitmap.
func (db *DB) hashDirty() { hashLeaves(db.tree, db.state, db.takeDirty(nil), db.pool) }
