package pagerdb

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
)

// The durable intent precedes all rewind mutations. Old log records remain intact
// until restored state is durable, so recovery can repeat the undo operation.
func (db *DB) writeRewindIntent(v, epoch uint64, end int64) error {
	var b [32]byte
	copy(b[:], "PGREWND2")
	le.PutUint64(b[8:], v)
	le.PutUint64(b[16:], epoch)
	le.PutUint64(b[24:], uint64(end))
	path := filepath.Join(db.dir, "rewind")
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	_, err = f.Write(b[:])
	if err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	if err = os.Rename(path+".tmp", path); err != nil {
		return err
	}
	return syncDir(db.dir)
}

func (db *DB) recoverRewind() error {
	path := filepath.Join(db.dir, "rewind")
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	v, epoch, end, err := parseRewindIntent(b)
	if err != nil {
		return err
	}
	des, err := os.ReadDir(filepath.Join(db.dir, "log"))
	if err != nil {
		return err
	}
	var epochs []uint64
	for _, de := range des {
		if e, er := strconv.ParseUint(de.Name(), 10, 64); er == nil && e >= epoch {
			epochs = append(epochs, e)
		}
	}
	slices.Sort(epochs)
	if len(epochs) == 0 || epochs[0] != epoch {
		return fmt.Errorf("pagerdb: rewind target log missing")
	}
	state, err := os.OpenFile(filepath.Join(db.dir, "state"), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer state.Close()
	fi, err := state.Stat()
	if err != nil {
		return err
	}
	for i := len(epochs) - 1; i >= 0; i-- {
		c, e := openChunk(db.chunkPath(epochs[i]), epochs[i])
		if e != nil {
			return e
		}
		if c == nil {
			return fmt.Errorf("pagerdb: rewind log checkpoint missing")
		}
		_, _, blocks, tail, ok := scan(c.m)
		if !ok {
			c.close()
			return fmt.Errorf("pagerdb: rewind log checkpoint missing")
		}
		var entries []int64
		for _, bl := range blocks {
			if bl.ver > v {
				entries = append(entries, bl.entries...)
			}
		}
		entries = append(entries, tail...)
		for j := len(entries) - 1; j >= 0; j-- {
			pos := entries[j]
			n, off := uint64(le.Uint32(c.m[pos+8:])), le.Uint64(c.m[pos+24:])
			if off > uint64(fi.Size()) || n > uint64(fi.Size())-off {
				c.close()
				return fmt.Errorf("pagerdb: rewind log state bounds")
			}
			if _, e = state.WriteAt(c.m[pos+entryHdr:pos+entryHdr+int64(n)], int64(off)); e != nil {
				c.close()
				return e
			}
		}
		c.close()
	}
	if err = state.Sync(); err != nil {
		return err
	}
	// Delete newest logs first. The target undo records remain until every later
	// log deletion is durable, preventing an interrupted retry from overshooting.
	for i := len(epochs) - 1; i > 0; i-- {
		if err = os.Remove(db.chunkPath(epochs[i])); err != nil {
			return err
		}
	}
	if err = syncDir(filepath.Join(db.dir, "log")); err != nil {
		return err
	}
	f, err := os.OpenFile(db.chunkPath(epoch), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	if end > uint64(st.Size()) {
		f.Close()
		return fmt.Errorf("pagerdb: rewind log position outside file")
	}
	var zeros [64 << 10]byte
	for at := end; at < uint64(st.Size()); {
		n := min(uint64(len(zeros)), uint64(st.Size())-at)
		if _, err = f.WriteAt(zeros[:n], int64(at)); err != nil {
			f.Close()
			return err
		}
		at += n
	}
	err = f.Sync()
	f.Close()
	if err != nil {
		return err
	}
	for latest := db.o.Store.Latest(); latest >= epoch; latest-- {
		if err = db.o.Store.Delete(latest); err != nil {
			return err
		}
	}
	if err = os.Remove(path); err != nil {
		return err
	}
	return syncDir(db.dir)
}

// parseRewindIntent checks the fixed-size rewind journal before filesystem access.
func parseRewindIntent(b []byte) (v, epoch, end uint64, err error) {
	if len(b) != 32 || string(b[:8]) != "PGREWND2" {
		return 0, 0, 0, fmt.Errorf("pagerdb: invalid rewind intent")
	}
	v, epoch, end = le.Uint64(b[8:]), le.Uint64(b[16:]), le.Uint64(b[24:])
	if epoch == 0 || end < cpRec || end > 1<<63-1 {
		return 0, 0, 0, fmt.Errorf("pagerdb: invalid rewind intent fields")
	}
	return v, epoch, end, nil
}
