package pagerdb

import (
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
)

func (vw *View) faultMap(r Region, l loc) (*viewMapping, error) { return nil, errFaultUnavailable }

// cloneState returns an unlinked copy-on-write clone of the state file, or an
// error if the filesystem cannot clone. Callers sync the mapping first.
func cloneState(db *DB) (*os.File, error) {
	dir, err := os.MkdirTemp(db.dir, ".view-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "state")
	if err = unix.Clonefile(db.stateF.Name(), path, 0); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_RDWR, 0)
}

func snapshotState(db *DB) (*os.File, error) {
	if f, err := cloneState(db); err == nil {
		return f, nil
	}
	f, err := os.CreateTemp(db.dir, ".view-")
	if err != nil {
		return nil, err
	}
	os.Remove(f.Name())
	if err = sparseSnapshot(db.stateF, f); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
