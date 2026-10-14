package pagerdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestExportImport(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14, EpochVersions: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(8192)
	if err != nil {
		t.Fatal(err)
	}
	roots := map[uint64][32]byte{}
	for i := 1; i <= 8; i++ {
		db.Write(r, i*100, bytes.Repeat([]byte{byte(i)}, 100))
		root, v, err := db.Commit()
		if err != nil {
			t.Fatal(err)
		}
		roots[v] = root
	}
	if err = db.Seal(); err != nil {
		t.Fatal(err)
	}
	for _, v := range []uint64{8, 7, 2} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			var image bytes.Buffer
			if err := db.Export(v, &image); err != nil {
				t.Fatal(err)
			}
			copyDB, err := Import(filepath.Join(t.TempDir(), "copy"), bytes.NewReader(image.Bytes()), roots[v])
			if err != nil {
				t.Fatal(err)
			}
			defer copyDB.Close()
			if copyDB.Root() != roots[v] || copyDB.Version() != v {
				t.Fatal("root or version differs")
			}
			vw, _ := db.View(v)
			defer vw.Close()
			want, _ := vw.Copy(r)
			got := make([]byte, len(want))
			copyDB.Read(r, 0, len(got), got)
			if !bytes.Equal(got, want) {
				t.Fatal("bytes differ")
			}
			bad := bytes.Clone(image.Bytes())
			bad[imageHeader+8+120] ^= 1
			if _, err := Import(filepath.Join(t.TempDir(), "bad"), bytes.NewReader(bad), roots[v]); err == nil {
				t.Fatal("corrupt image accepted")
			}
			if _, err := Import(filepath.Join(t.TempDir(), "wrong"), bytes.NewReader(image.Bytes()), [32]byte{}); err == nil {
				t.Fatal("wrong expected root accepted")
			}
		})
	}
}

type discardAt struct{}

func (discardAt) WriteAt(b []byte, off int64) (int, error) { return len(b), nil }
func FuzzExportImage(f *testing.F) {
	f.Add([]byte(imageMagic))
	f.Add(make([]byte, imageHeader))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			return
		}
		var root [32]byte
		if len(b) >= imageHeader {
			copy(root[:], b[32:64])
		}
		readImage(bytes.NewReader(b), discardAt{}, root)
	})
}
func TestImageTreeSparse(t *testing.T) {
	a := newImageTree()
	b := newImageTree()
	var leaf [LeafSize]byte
	leaf[1] = 12
	for i := uint64(0); i < 1<<14; i++ {
		h := a.empty[0]
		if i == 101 {
			h = sha256Sum(leaf[:])
		}
		a.add(h, 0)
	}
	b.zerosTo(101)
	b.add(sha256Sum(leaf[:]), 0)
	b.zerosTo(1 << 14)
	if a.stack[14] != b.stack[14] {
		t.Fatal("sparse fold differs")
	}
}
func Example() {
	dir, err := os.MkdirTemp("", "pagerdb-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	db, err := Open(dir, Options{MaxLeaves: 1 << 14})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	r, err := db.Alloc(1024)
	if err != nil {
		panic(err)
	}
	db.Write(r, 0, []byte("hello"))
	root, v, err := db.Commit()
	if err != nil {
		panic(err)
	}
	view, err := db.View(v)
	if err != nil {
		panic(err)
	}
	defer view.Close()
	b := make([]byte, 5)
	if err = view.Read(r, 0, 5, b); err != nil {
		panic(err)
	}
	proof, err := db.Proof(r, 0)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b), Verify(root, proof))
	// Output: hello true
}
