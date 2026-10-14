package pagerdb

// Temporary stand-ins for recovery, views and epochs, replaced in the next steps.

import "io"

type viewState struct{ pins map[uint64]int }

type sealState struct{ sealErr error }

type EpochStore interface {
	Put(epoch uint64, r io.Reader) error
	Open(epoch uint64) (io.ReaderAt, int64, error)
	Latest() uint64
}

type Mark struct{}

func NewLocalStore(string) (EpochStore, error) { return nil, nil }

func (db *DB) mark() Mark            { return Mark{} }
func (db *DB) undo(Mark)             {}
func (db *DB) undoTo(pos, seq int64) {}
func (db *DB) stopSealer()           {}
func (db *DB) closeSealed()          {}
func (db *DB) enqueueSeal(*chunk)    {}

// open always creates a fresh store until recovery exists.
func (db *DB) open() error {
	db.heads = map[uint32]int64{}
	db.rootIdx = map[[32]byte]uint64{}
	if err := db.mapFiles(true); err != nil {
		return err
	}
	db.root = db.hashDirty()
	c, err := db.newChunk(1, 0, db.root)
	if err != nil {
		return err
	}
	db.cur, db.chunks = c, []*chunk{c}
	db.blocks = []block{{ver: 0, root: db.root, c: c, end: cpRec}}
	db.blockStart = cpRec
	return nil
}
