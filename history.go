package pagerdb

import (
	"fmt"
	"golang.org/x/sys/unix"
)

// Rewind restores a version in the RAM window and durably checkpoints it before
// returning. It invalidates Marks and mapped slices; existing Views are detached.
func (db *DB) Rewind(v uint64) error {
	db.check()
	if len(db.undo) != 0 {
		return ErrPending
	}
	root, err := db.RootAt(v)
	if err != nil {
		return err
	}
	if err = db.Flush(); err != nil {
		return err
	}
	parent := db.version
	for len(db.history) > 0 && db.history[len(db.history)-1].version > v {
		i := len(db.history) - 1
		restore(db.state, db.history[i].undo, db.markDirty)
		db.history[i].retained = false
		db.history[i] = nil
		db.history = db.history[:i]
	}
	db.version = v
	db.generation++
	b := db.snapshot()
	b.version = v
	b.parent = parent
	b.rewind = true
	b.checkpoint = true
	db.latest = b
	b.journal = true
	b.undo = b.undo[:0]
	b.ready.Add(1)
	b.persisted.Add(1)
	db.jobs <- b
	b.ready.Wait()
	if b.root != root {
		return fmt.Errorf("pagerdb: rewind root mismatch")
	}
	b.persisted.Wait()
	return db.disk.failure()
}

// View owns a private copy. Creation costs the current allocated state plus
// retained undo replay. Reads and maps need no DB locks or lifetime pinning.
type View struct {
	state   []byte
	version uint64
	root    [32]byte
}

func (db *DB) View(v uint64) (*View, error) {
	db.check()
	root, err := db.RootAt(v)
	if err != nil {
		return nil, err
	}
	state, err := anonymous(len(db.state))
	if err != nil {
		return nil, err
	}
	copy(state[:dataStart(db.o.MaxLeaves)], db.state[:dataStart(db.o.MaxLeaves)])
	for r := Region(1); uint64(r) < le.Uint64(db.state[16:]); r++ {
		off, n, e := location(db.state, r)
		if e == nil {
			copy(state[off:off+align(n)], db.state[off:off+align(n)])
		}
	}
	restore(state, db.undo, nil)
	for i := len(db.history) - 1; i >= 0 && db.history[i].version > v; i-- {
		restore(state, db.history[i].undo, nil)
	}
	return &View{state: state, version: v, root: root}, nil
}
func (v *View) Version() uint64 { return v.version }
func (v *View) Root() [32]byte  { return v.root }
func (v *View) Map(r Region) ([]byte, error) {
	if v.state == nil {
		return nil, ErrClosed
	}
	off, n, err := location(v.state, r)
	if err != nil {
		return nil, err
	}
	return v.state[off : off+n : off+n], nil
}
func (v *View) Read(r Region, off, n int, dst []byte) error {
	data, err := v.Map(r)
	if err != nil {
		return err
	}
	if off < 0 || n < 0 || off > len(data) || n > len(data)-off || len(dst) < n {
		return fmt.Errorf("pagerdb: view range out of bounds")
	}
	copy(dst, data[off:off+n])
	return nil
}
func (v *View) Close() error {
	if v.state == nil {
		return nil
	}
	err := unix.Munmap(v.state)
	v.state = nil
	return err
}
