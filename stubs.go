package pagerdb

// Temporary stand-ins for views and epochs, replaced in the next steps.

import (
	"io"
	"sync"
)

type viewState struct{ pins map[uint64]int }

type sealState struct {
	sealCond sync.Cond
	sealDone chan struct{}
	sealErr  error
}

type EpochStore interface {
	Put(epoch uint64, r io.Reader) error
	Open(epoch uint64) (io.ReaderAt, int64, error)
	Latest() uint64
}

type noStore struct{}

func (noStore) Put(uint64, io.Reader) error             { return nil }
func (noStore) Open(uint64) (io.ReaderAt, int64, error) { return nil, 0, io.EOF }
func (noStore) Latest() uint64                          { return 0 }
func NewLocalStore(string) (EpochStore, error)          { return noStore{}, nil }
func (db *DB) stopSealer()                              {}
func (db *DB) closeSealed()                             {}
func (db *DB) enqueueSeal(*chunk)                       {}
func (db *DB) sealer()                                  {}
func (db *DB) loadSealed() error                        { return nil }
