package pagerdb

// Temporary stand-ins for epochs, replaced in the next steps.

import (
	"io"
	"sync"
)

type sealState struct {
	sealed   []*sealedEpoch
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

type sealedEpoch struct{ end uint64 }

func (s *sealedEpoch) findRoot([32]byte) (uint64, bool, error) { return 0, false, nil }
func (db *DB) sealedLeaf(leaf, v uint64, out []byte) error     { return ErrVersion }
