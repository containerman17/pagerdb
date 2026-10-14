package pagerdb

import (
	"golang.org/x/sys/unix"
	"os"
)

// cloneState returns an unlinked reflink clone of the state file, or an error
// if the filesystem cannot clone. Callers sync the mapping first.
func cloneState(db *DB) (*os.File, error) {
	f, err := os.CreateTemp(db.dir, ".view-")
	if err != nil {
		return nil, err
	}
	os.Remove(f.Name())
	if err = unix.IoctlFileClone(int(f.Fd()), int(db.stateF.Fd())); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
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
