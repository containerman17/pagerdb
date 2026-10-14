package pagerdb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSparseSnapshot(t *testing.T) {
	dir := t.TempDir()
	src, err := os.Create(filepath.Join(dir, "source"))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	const end = 64 << 20
	if err = src.Truncate(end); err != nil {
		t.Fatal(err)
	}
	if _, err = src.WriteAt([]byte("last"), end-4); err != nil {
		t.Fatal(err)
	}
	dst, err := os.Create(filepath.Join(dir, "copy"))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if err = sparseSnapshot(src, dst); err != nil {
		t.Fatal(err)
	}
	fi, _ := dst.Stat()
	if fi.Size() != end {
		t.Fatal("wrong copy size")
	}
	var b [4]byte
	if _, err = dst.ReadAt(b[:], end-4); err != nil || string(b[:]) != "last" {
		t.Fatal("lost extent", err)
	}
	if _, err = dst.ReadAt(b[:], 0); err != nil || b != [4]byte{} {
		t.Fatal("nonzero hole", err)
	}
}
