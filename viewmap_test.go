package pagerdb

import (
	"bytes"
	"math/rand/v2"
	"sync"
	"testing"
)

func TestViewMap(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 15, EpochVersions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, size := range []int{1024, 5 << 20} {
		r, err := db.Alloc(size)
		if err != nil {
			t.Fatal(err)
		}
		db.Write(r, 0, []byte("old"))
		_, v, err := db.Commit()
		if err != nil {
			t.Fatal(err)
		}
		db.Write(r, 0, []byte("new"))
		if _, _, err = db.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = db.Seal(); err != nil {
			t.Fatal(err)
		}
		vw, _ := db.View(v)
		mp, err := vw.Map(r)
		if err != nil {
			t.Fatal(err)
		}
		if string(mp[:3]) != "old" {
			t.Fatal("old mapping differs")
		}
		rng := rand.New(rand.NewPCG(1, 3))
		var b [64]byte
		for range 200 {
			off := rng.IntN(len(mp) - len(b))
			if err = vw.Read(r, off, len(b), b[:]); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(mp[off:off+len(b)], b[:]) {
				t.Fatal("Map and Read differ")
			}
		}
		db.Write(r, 128, []byte("later"))
		if _, _, err = db.Commit(); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(mp[128:133], make([]byte, 5)) {
			t.Fatal("future write reached map")
		}
		copy(mp[:3], "own")
		db.Read(r, 0, 3, b[:])
		if string(b[:3]) != "new" {
			t.Fatal("private write reached DB")
		}
		vw.Close()
	}
}
func TestParallelViewMaps(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, _ := db.Alloc(1 << 20)
	db.Write(r, 123, []byte("value"))
	_, v, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	var views []*View
	var maps [][]byte
	for range 8 {
		vw, _ := db.View(v)
		m, err := vw.Map(r)
		if err != nil {
			t.Fatal(err)
		}
		views = append(views, vw)
		maps = append(maps, m)
	}
	var wg sync.WaitGroup
	for i, m := range maps {
		wg.Go(func() {
			if string(m[123:128]) != "value" {
				t.Error("wrong map bytes")
			}
			m[123] = byte(i)
		})
	}
	wg.Wait()
	for _, vw := range views {
		vw.Close()
	}
	var b [5]byte
	db.Read(r, 123, 5, b[:])
	if string(b[:]) != "value" {
		t.Fatal("private writes reached DB")
	}
}

func TestPrivateMapFallback(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, _ := db.Alloc(1024)
	db.Write(r, 0, []byte("old"))
	_, v, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	vw, _ := db.View(v)
	l, _ := vw.loc(r)
	m, err := vw.privateMap(l)
	if err != nil {
		t.Fatal(err)
	}
	vw.mappings = append(vw.mappings, m)
	defer vw.Close()
	db.Write(r, 0, []byte("new"))
	if string(m.data[:3]) != "old" {
		t.Fatal("fallback snapshot changed")
	}
	m.data[0] = 'x'
	var b [3]byte
	db.Read(r, 0, 3, b[:])
	if string(b[:]) != "new" {
		t.Fatal("private write escaped")
	}
}
