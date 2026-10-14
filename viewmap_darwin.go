package pagerdb

import (
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
)

func (vw *View) faultMap(r Region, l loc) (*viewMapping, error) { return nil, errFaultUnavailable }
func snapshotState(db *DB) (*os.File, error) {
	dir, err := os.MkdirTemp(db.dir, ".view-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "state")
	if err = unix.Clonefile(db.stateF.Name(), path, 0); err == nil {
		return os.Open(path)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if err = sparseSnapshot(db.stateF, f); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
