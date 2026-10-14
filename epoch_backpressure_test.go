package pagerdb

import (
	"errors"
	"io"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

type gatedEpochStore struct {
	EpochStore
	started chan struct{}
	release chan struct{}
	failure error
}

func (s *gatedEpochStore) Put(epoch uint64, r io.Reader) error {
	if epoch == 1 {
		close(s.started)
		<-s.release
		if s.failure != nil {
			return s.failure
		}
	}
	return s.EpochStore.Put(epoch, r)
}

func TestSealBackpressure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			local, err := NewLocalStore(filepath.Join(dir, "epochs"))
			if err != nil {
				t.Fatal(err)
			}
			store := &gatedEpochStore{EpochStore: local, started: make(chan struct{}), release: make(chan struct{})}
			sentinel := errors.New("gated storage failure")
			if fail {
				store.failure = sentinel
			}
			var once sync.Once
			release := func() { once.Do(func() { close(store.release) }) }
			db, err := Open(dir, Options{MaxLeaves: 1 << 16, EpochVersions: 1, Store: store})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { release(); db.Close() }()
			if _, _, err = db.Commit(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-store.started:
			case <-time.After(10 * time.Second):
				t.Fatal("first Put did not start")
			}
			if _, _, err = db.Commit(); err != nil {
				t.Fatal(err)
			}
			type result struct {
				v   uint64
				err error
			}
			done := make(chan result, 1)
			go func() { _, v, e := db.Commit(); done <- result{v, e} }()
			deadline := time.Now().Add(10 * time.Second)
			for {
				db.mu.Lock()
				waiting := db.version == 3 && db.cur.epoch == 3 && db.sealPending == 2
				db.mu.Unlock()
				if waiting {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("third commit did not wait at rotation")
				}
				runtime.Gosched()
			}
			select {
			case r := <-done:
				t.Fatalf("commit returned before Put finished: %+v", r)
			default:
			}
			release()
			select {
			case r := <-done:
				if r.v != 3 {
					t.Fatalf("committed version %d", r.v)
				}
				if fail && !errors.Is(r.err, sentinel) {
					t.Fatalf("missing storage error: %v", r.err)
				}
				if !fail && r.err != nil {
					t.Fatal(r.err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("commit did not wake after Put")
			}
			db.sealWG.Wait()
			db.mu.Lock()
			pending, epoch := db.sealPending, db.cur.epoch
			db.mu.Unlock()
			if pending != 0 {
				t.Fatalf("pending after drain: %d", pending)
			}
			if fail {
				if epoch != 3 {
					t.Fatalf("failed rotation created epoch %d", epoch)
				}
				if _, _, err = db.Commit(); !errors.Is(err, sentinel) {
					t.Fatalf("next commit: %v", err)
				}
			} else if epoch != 4 {
				t.Fatalf("successful rotation epoch %d", epoch)
			}
		})
	}
}
