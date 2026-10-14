package pagerdb

import (
	"golang.org/x/sys/unix"
	"os"
)

func snapshotState(db *DB) (*os.File, error) {
	f, err := os.CreateTemp(db.dir, ".view-")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	if err = unix.IoctlFileClone(int(f.Fd()), int(db.stateF.Fd())); err != nil {
		err = sparseSnapshot(db.stateF, f)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
