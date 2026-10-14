package pagerdb

// Temporary stand-ins for the log, tree, views and epochs, replaced in the next steps.

import (
	"io"
	"os"
	"path/filepath"
)

type logState struct {
	cur        *chunk
	chunks     []*chunk
	blockStart int64
	blockSeq   int64
	touches    []int64
}

type chunk struct {
	m    []byte
	tail int64
}

func (c *chunk) close() {}

type viewState struct{}

type sealState struct{ sealErr error }

type EpochStore interface {
	Put(epoch uint64, r io.Reader) error
	Open(epoch uint64) (io.ReaderAt, int64, error)
	Latest() uint64
}

type Mark struct{}

func NewLocalStore(string) (EpochStore, error) { return nil, nil }

func (db *DB) put(_ Region, fo uint64, b []byte) { copy(db.state[fo:], b) }
func (db *DB) markDirty(off, n uint64)           {}
func (db *DB) mark() Mark                        { return Mark{} }
func (db *DB) undo(Mark)                         {}
func (db *DB) undoTo(pos, seq int64)             {}
func (db *DB) stopSealer()                       {}
func (db *DB) closeSealed()                      {}
func sha256Sum([]byte) [32]byte                  { return [32]byte{} }
func hashPair(a, b []byte) [32]byte              { return [32]byte{} }

func (db *DB) open() error {
	_, err := os.Stat(filepath.Join(db.dir, "state"))
	return db.mapFiles(err != nil)
}
