package pagerdb

import (
	"bytes"
	"io"
	"os"
	"runtime"
	"testing"
)

func TestDiskSortPieces(t *testing.T) {
	dir := t.TempDir()
	f, err := os.CreateTemp(dir, "refs-")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var r [40]byte
	for i := 0; i < 100003; i++ {
		le.PutUint64(r[:], uint64((i*7919)%257))
		le.PutUint64(r[8:], uint64(i))
		if _, err = f.Write(r[:]); err != nil {
			t.Fatal(err)
		}
	}
	sorted, err := diskSortPieces(dir, f)
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Close()
	var prev [40]byte
	for i := 0; i < 100003; i++ {
		if _, err = io.ReadFull(sorted, r[:]); err != nil {
			t.Fatal(err)
		}
		if i > 0 && (le.Uint64(r[:]) < le.Uint64(prev[:]) || le.Uint64(r[:]) == le.Uint64(prev[:]) && le.Uint64(r[8:]) >= le.Uint64(prev[8:])) {
			t.Fatalf("unsorted record %d", i)
		}
		prev = r
	}
	if _, err = io.ReadFull(sorted, r[:]); err != io.EOF {
		t.Fatalf("trailing record: %v", err)
	}
}

func TestParallelSealMatchesSerial(t *testing.T) {
	original := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(original)
	var want [32]byte
	var expected bytes.Buffer
	for _, workers := range []int{1, 4} {
		runtime.GOMAXPROCS(workers)
		db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 17, EpochVersions: 1000})
		if err != nil {
			t.Fatal(err)
		}
		region, err := db.Alloc(128 * LeafSize)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		var value [64]byte
		for version := 1; version <= 160; version++ {
			for leaf := 0; leaf < 128; leaf++ {
				le.PutUint64(value[:], uint64(version*1000+leaf))
				db.Write(region, leaf*LeafSize, value[:])
			}
			if _, _, err = db.Commit(); err != nil {
				db.Close()
				t.Fatal(err)
			}
		}
		if err = db.Seal(); err != nil {
			db.Close()
			t.Fatal(err)
		}
		if workers == 1 {
			want = db.Root()
		} else if want != db.Root() {
			t.Fatal("parallel root mismatch")
		}
		var image bytes.Buffer
		if err = db.Export(80, &image); err != nil {
			db.Close()
			t.Fatal(err)
		}
		if workers == 1 {
			expected.Write(image.Bytes())
		} else if !bytes.Equal(expected.Bytes(), image.Bytes()) {
			t.Fatal("parallel history differs")
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
