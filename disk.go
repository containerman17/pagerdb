package pagerdb

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"golang.org/x/sys/unix"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

const checkpointHeader = 64

type diskWriter struct {
	db           *DB
	image        []byte
	changed      []uint32
	bits         []uint64
	file         *os.File
	journal      *os.File
	journalBytes int64
	epoch        uint64
	end          int64
	mu           sync.Mutex
	cond         *sync.Cond
	queue        [snapshotCount + 1]*block
	queueHead    int
	queueLength  int
	record       []byte
	stopping     bool
	err          error
	done         chan struct{}
}

func openDisk(db *DB) (_ *diskWriter, err error) {
	d := &diskWriter{db: db, bits: make([]uint64, len(db.dirtyBits)), done: make(chan struct{})}
	d.cond = sync.NewCond(&d.mu)
	defer func() {
		if err != nil {
			if d.file != nil {
				d.file.Close()
			}
			if d.journal != nil {
				d.journal.Close()
			}
			if d.image != nil {
				unix.Munmap(d.image)
			}
		}
	}()
	d.image, err = anonymous(len(db.state))
	if err != nil {
		return nil, err
	}
	d.file, err = os.OpenFile(filepath.Join(db.dir, "checkpoint"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	manifest, e := os.ReadFile(filepath.Join(db.dir, "CURRENT"))
	if e == nil {
		if len(manifest) != 64 || string(manifest[:8]) != "PGCUR003" || crc32.Checksum(manifest[:60], crcTable) != le.Uint32(manifest[60:]) {
			return nil, fmt.Errorf("pagerdb: invalid checkpoint manifest")
		}
		d.end = int64(le.Uint64(manifest[8:]))
		if d.end < 68 {
			return nil, fmt.Errorf("pagerdb: invalid checkpoint end")
		}
		st, e := d.file.Stat()
		if e != nil {
			return nil, e
		}
		if st.Size() < d.end {
			return nil, io.ErrUnexpectedEOF
		}
		if err = d.load(manifest); err != nil {
			return nil, err
		}
		db.version = le.Uint64(manifest[16:])
		want := [32]byte(manifest[24:56])
		// d.changed contains each restored leaf once; rebuild from final state.
		slices.Sort(d.changed)
		for first := 0; first < len(d.changed); first += 1024 {
			ids := d.changed[first:min(first+1024, len(d.changed))]
			data := make([]byte, len(ids)*LeafSize)
			for i, id := range ids {
				off := uint64(id) * LeafSize
				copy(data[i*LeafSize:], db.state[off:off+LeafSize])
			}
			db.tree.update(ids, data, db.pool)
		}
		if db.tree.root() != want {
			return nil, fmt.Errorf("pagerdb: checkpoint root mismatch")
		}
		if err = validateAlloc(db.state, db.o.MaxLeaves); err != nil {
			return nil, err
		}
		d.resetChanged()
	} else if errors.Is(e, os.ErrNotExist) {
		entries, readErr := os.ReadDir(filepath.Join(db.dir, "log"))
		if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			return nil, readErr
		}
		if len(entries) != 0 {
			return nil, fmt.Errorf("pagerdb: missing checkpoint manifest in an existing store")
		}
		// A checkpoint tail without CURRENT was never published. Start at genesis.
		copy(db.state, "PAGER003")
		le.PutUint64(db.state[8:], db.o.MaxLeaves)
		le.PutUint64(db.state[16:], 1)
		copy(d.image[:LeafSize], db.state[:LeafSize])
		d.mark(0)
		db.tree.update([]uint32{0}, db.state[:LeafSize], db.pool)
		if err = d.checkpoint(0, db.tree.root()); err != nil {
			return nil, err
		}
	} else {
		return nil, e
	}
	if err = d.file.Truncate(d.end); err != nil {
		return nil, err
	}
	if _, err = d.file.Seek(d.end, io.SeekStart); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Join(db.dir, "log"), 0755); err != nil {
		return nil, err
	}
	entries, e := os.ReadDir(filepath.Join(db.dir, "log"))
	if e != nil {
		return nil, e
	}
	for _, ent := range entries {
		var n uint64
		if _, e := fmt.Sscanf(ent.Name(), "%020d.journal", &n); e == nil {
			d.epoch = max(d.epoch, n)
		}
	}
	if err = d.rotate(); err != nil {
		return nil, err
	}
	return d, nil
}
func (d *diskWriter) mark(id uint32) {
	mask := uint64(1) << (id % 64)
	if d.bits[id/64]&mask == 0 {
		d.bits[id/64] |= mask
		d.changed = append(d.changed, id)
	}
}
func (d *diskWriter) resetChanged() {
	for _, id := range d.changed {
		d.bits[id/64] &= ^(uint64(1) << (id % 64))
	}
	d.changed = d.changed[:0]
}
func (d *diskWriter) load(manifest []byte) error {
	var off int64
	var header [64]byte
	var rec [4 + LeafSize]byte
	for off < d.end {
		if d.end-off < 68 {
			return io.ErrUnexpectedEOF
		}
		if _, e := d.file.ReadAt(header[:], off); e != nil {
			return e
		}
		off += 64
		if string(header[:8]) != "PGCKP003" || le.Uint64(header[8:]) != d.db.o.MaxLeaves {
			return fmt.Errorf("pagerdb: checkpoint format or capacity mismatch")
		}
		count := le.Uint64(header[24:])
		if count > d.db.o.MaxLeaves || count > uint64((d.end-off-4)/(4+LeafSize)) {
			return fmt.Errorf("pagerdb: checkpoint count out of bounds")
		}
		h := crc32.New(crcTable)
		h.Write(header[:])
		var prev uint32
		for i := uint64(0); i < count; i++ {
			if _, e := d.file.ReadAt(rec[:], off); e != nil {
				return e
			}
			off += int64(len(rec))
			h.Write(rec[:])
			id := le.Uint32(rec[:])
			if uint64(id) >= d.db.o.MaxLeaves || (i > 0 && id <= prev) {
				return fmt.Errorf("pagerdb: invalid checkpoint leaf order")
			}
			prev = id
			p := uint64(id) * LeafSize
			copy(d.db.state[p:p+LeafSize], rec[4:])
			copy(d.image[p:p+LeafSize], rec[4:])
			d.mark(id)
		}
		var sum [4]byte
		if _, e := d.file.ReadAt(sum[:], off); e != nil {
			return e
		}
		off += 4
		if le.Uint32(sum[:]) != h.Sum32() {
			return fmt.Errorf("pagerdb: checkpoint checksum mismatch")
		}
		if off == d.end && (le.Uint64(header[16:]) != le.Uint64(manifest[16:]) || [32]byte(header[32:]) != [32]byte(manifest[24:56])) {
			return fmt.Errorf("pagerdb: checkpoint header differs from manifest")
		}
	}
	return nil
}
func (d *diskWriter) checkpoint(v uint64, root [32]byte) error {
	slices.Sort(d.changed)
	// Incremental frames are immutable once CURRENT publishes their end offset.
	header := make([]byte, 64)
	copy(header, "PGCKP003")
	le.PutUint64(header[8:], d.db.o.MaxLeaves)
	le.PutUint64(header[16:], v)
	le.PutUint64(header[24:], uint64(len(d.changed)))
	copy(header[32:], root[:])
	if _, err := d.file.Seek(d.end, io.SeekStart); err != nil {
		return err
	}
	h := crc32.New(crcTable)
	write := func(b []byte) error { h.Write(b); _, err := d.file.Write(b); return err }
	if err := write(header); err != nil {
		return err
	}
	// Batch checkpoint records to avoid one syscall per leaf.
	buf := make([]byte, 0, (4+LeafSize)*256)
	for i, id := range d.changed {
		buf = le.AppendUint32(buf, id)
		p := uint64(id) * LeafSize
		buf = append(buf, d.image[p:p+LeafSize]...)
		if len(buf) == cap(buf) || i == len(d.changed)-1 {
			if err := write(buf); err != nil {
				return err
			}
			buf = buf[:0]
		}
	}
	if _, err := d.file.Write(le.AppendUint32(nil, h.Sum32())); err != nil {
		return err
	}
	if err := dataSync(int(d.file.Fd())); err != nil {
		return err
	}
	end := d.end + 64 + int64(len(d.changed))*(4+LeafSize) + 4
	manifest := make([]byte, 64)
	copy(manifest, "PGCUR003")
	le.PutUint64(manifest[8:], uint64(end))
	le.PutUint64(manifest[16:], v)
	copy(manifest[24:], root[:])
	le.PutUint32(manifest[60:], crc32.Checksum(manifest[:60], crcTable))
	path := filepath.Join(d.db.dir, "CURRENT.tmp")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(manifest)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(path, filepath.Join(d.db.dir, "CURRENT")); err != nil {
		return err
	}
	dir, err := os.Open(d.db.dir)
	if err != nil {
		return err
	}
	err = dir.Sync()
	err = errors.Join(err, dir.Close())
	if err != nil {
		return err
	}
	d.end = end
	d.resetChanged()
	return nil
}
func (d *diskWriter) rotate() error {
	if d.journal != nil {
		if err := d.journal.Close(); err != nil {
			return err
		}
	}
	d.epoch++
	d.journalBytes = 0
	var err error
	d.journal, err = os.OpenFile(filepath.Join(d.db.dir, "log", fmt.Sprintf("%020d.journal", d.epoch)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	return err
}
func (d *diskWriter) append(b *block) error {
	// Header: magic, frame bytes, version, parent version, root, undo bytes,
	// flags (1 = rewind), reserved. Footer CRC-32C covers header and undo.
	record := slices.Grow(d.record[:0], 88+len(b.undo)+4)[:88]
	clear(record)
	copy(record, "PGJNL003")
	le.PutUint64(record[8:], uint64(88+len(b.undo)+4))
	le.PutUint64(record[16:], b.version)
	le.PutUint64(record[24:], b.parent)
	copy(record[32:64], b.root[:])
	le.PutUint64(record[64:], uint64(len(b.undo)))
	if b.rewind {
		le.PutUint64(record[72:], 1)
	}
	record = append(record, b.undo...)
	record = le.AppendUint32(record, crc32.Checksum(record, crcTable))
	d.record = record
	if d.journalBytes > 0 && d.journalBytes+int64(len(record)) > d.db.o.EpochBytes {
		if err := d.rotate(); err != nil {
			return err
		}
	}
	n, err := d.journal.Write(record)
	d.journalBytes += int64(n)
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
	d.cond.Signal()
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
			if b.journal {
				err = d.append(b)
			}
			if err == nil {
				for i, id := range b.ids {
					p := uint64(id) * LeafSize
					copy(d.image[p:p+LeafSize], b.data[i*LeafSize:(i+1)*LeafSize])
					d.mark(id)
				}
				if b.checkpoint {
					err = d.checkpoint(b.version, b.root)
				}
			}
		}
		b.data = nil
		b.ids = nil
		slot := b.snapshot
		b.snapshot = nil
		if err != nil {
			d.mu.Lock()
			d.err = err
			d.mu.Unlock()
		}
		if slot != nil {
			slot.done.Done()
		}
		b.persisted.Done()
	}
	err := errors.Join(d.journal.Close(), d.file.Close(), unix.Munmap(d.image))
	d.mu.Lock()
	d.err = errors.Join(d.err, err)
	d.mu.Unlock()
}
func (d *diskWriter) stop() { d.mu.Lock(); d.stopping = true; d.cond.Signal(); d.mu.Unlock(); <-d.done }

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
	// Bound scratch memory when verifying a large region.
	for len(ids) > 0 {
		n := min(len(ids), 1024)
		data := make([]byte, n*LeafSize)
		for i, id := range ids[:n] {
			p := uint64(id) * LeafSize
			copy(data[i*LeafSize:], db.state[p:p+LeafSize])
		}
		t.update(ids[:n], data, db.pool)
		ids = ids[n:]
	}
	return t.root(), nil
}
