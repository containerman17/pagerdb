package pagerdb

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
)

var smallSizes = []int{1, 100, 1024, 1500, 4096, 10000, 16 << 10, 100 << 10}

// checkView reads random ranges of every region alive at the view's version.
func checkView(t *testing.T, rng *rand.Rand, vw *View, m model) {
	t.Helper()
	for r, want := range m {
		n := 1 + rng.IntN(min(len(want), 3000))
		off := rng.IntN(len(want) - n + 1)
		got := make([]byte, n)
		if err := vw.Read(r, off, n, got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want[off:off+n]) {
			t.Fatalf("view %d region %d [%d,%d): bytes differ", vw.Version(), r, off, off+n)
		}
	}
	for r, want := range m {
		got, err := vw.Copy(r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("view %d region %d: Copy differs", vw.Version(), r)
		}
		break
	}
}

func TestViews(t *testing.T) {
	for _, ev := range []uint64{7, 1000} {
		t.Run(fmt.Sprint("epoch", ev), func(t *testing.T) { testViews(t, ev) })
	}
}

func testViews(t *testing.T, epochVersions uint64) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14, EpochVersions: epochVersions})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rng := rand.New(rand.NewPCG(3, 4))
	m := model{}
	snaps := []model{{}}
	roots := [][32]byte{db.Root()}
	type open struct {
		vw *View
		m  model
	}
	var held []open
	for block := 0; block < 80; block++ {
		for range rng.IntN(30) {
			step(t, rng, db, m, smallSizes)
		}
		// Views opened earlier must stay correct while the head moves.
		for _, h := range held {
			checkView(t, rng, h.vw, h.m)
		}
		root, _, err := db.Commit()
		if err != nil {
			t.Fatal(err)
		}
		snaps = append(snaps, m.clone())
		roots = append(roots, root)
		if rng.IntN(4) == 0 {
			v := uint64(rng.IntN(len(snaps)))
			vw, err := db.View(v)
			if err != nil {
				t.Fatal(err)
			}
			held = append(held, open{vw, snaps[v]})
		}
		if len(held) > 3 {
			held[0].vw.Close()
			held = held[1:]
		}
	}
	for _, h := range held {
		h.vw.Close()
	}
	all := func() (sealedViews int) {
		for v := range snaps {
			vw, err := db.View(uint64(v))
			if err != nil {
				t.Fatal(err)
			}
			if !vw.window {
				sealedViews++
			}
			checkView(t, rng, vw, snaps[v])
			vw.Close()
			vr, err := db.ViewRoot(roots[v])
			if err != nil {
				t.Fatalf("ViewRoot(version %d): %v", v, err)
			}
			if roots[vr.Version()] != roots[v] {
				t.Fatalf("ViewRoot(version %d) opened version %d with another root", v, vr.Version())
			}
			vr.Close()
		}
		return sealedViews
	}
	all()
	if err := db.Seal(); err != nil {
		t.Fatal(err)
	}
	sealedViews := all()
	if sealedViews < len(snaps)/2 {
		t.Fatalf("only %d of %d versions read from sealed epochs", sealedViews, len(snaps))
	}
	t.Logf("%d versions, %d from sealed epochs, %d sealed epochs, window %d chunks", len(snaps), sealedViews, len(db.sealed), len(db.chunks))
}
