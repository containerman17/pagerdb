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
	db.sealWG.Wait()
	if db.sealErr != nil {
		t.Fatal(db.sealErr)
	}
	for _, v := range []uint64{8, 7, 2} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			probe, err := db.View(v)
			if err != nil {
				t.Fatal(err)
			}
			if probe.window != (v >= 7) {
				t.Fatal("unexpected history path")
			}
			probe.Close()
			var image bytes.Buffer
			if err := db.Export(v, &image); err != nil {
				t.Fatal(err)
			}
			copyDB, err := Import(filepath.Join(t.TempDir(), "copy"), bytes.NewReader(image.Bytes()), roots[v])
			if err != nil {
				t.Fatal(err)
			}
			defer copyDB.Close()
			if _, err := copyDB.View(v - 1); err == nil {
				t.Fatal("import exposed missing history")
			}
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

func TestImageParserRejectsMalformed(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var buf bytes.Buffer
	if err = db.Export(0, &buf); err != nil {
		t.Fatal(err)
	}
	good := buf.Bytes()
	cases := map[string][]byte{
		"header truncated":   good[:20],
		"leaf truncated":     good[:imageHeader+50],
		"trailing bytes":     append(bytes.Clone(good), 0),
		"missing terminator": good[:len(good)-8],
	}
	wrongOrder := bytes.Clone(good)
	le.PutUint64(wrongOrder[imageHeader:], 1)
	cases["missing header leaf"] = wrongOrder
	badCapacity := bytes.Clone(good)
	le.PutUint64(badCapacity[8:], 123)
	cases["capacity"] = badCapacity
	badUsed := bytes.Clone(good)
	le.PutUint64(badUsed[24:], 1)
	cases["state length"] = badUsed
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := readImage(bytes.NewReader(b), discardAt{}, db.Root()); err == nil {
				t.Fatal("accepted malformed image")
			}
		})
	}
	if err = db.Export(0, shortWriter{}); err == nil {
		t.Fatal("accepted short writer")
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return 0, nil }

func TestExportAfterRetentionPrunesLogs(t *testing.T) {
	dir := t.TempDir()
	o := Options{MaxLeaves: 1 << 14, EpochVersions: 2, RetainVersions: 3}
	db, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Alloc(1024)
	if err != nil {
		t.Fatal(err)
	}
	var first [32]byte
	for i := byte(1); i <= 10; i++ {
		db.Write(r, 0, []byte{i})
		root, _, err := db.Commit()
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			first = root
		}
	}
	if err = db.Seal(); err != nil {
		t.Fatal(err)
	}
	if db.chunks[0].start == 0 {
		t.Fatal("retention did not prune logs")
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	vw, err := db.View(1)
	if err != nil {
		t.Fatal(err)
	}
	defer vw.Close()
	var got [1]byte
	if err = vw.Read(r, 0, 1, got[:]); err != nil || got[0] != 1 {
		t.Fatalf("sealed view %v %v", got, err)
	}
	var image bytes.Buffer
	if err = db.Export(1, &image); err != nil {
		t.Fatal(err)
	}
	imported, err := Import(filepath.Join(t.TempDir(), "copy"), &image, first)
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Close()
	imported.Read(r, 0, 1, got[:])
	if got[0] != 1 {
		t.Fatal("sealed export differs")
	}
}
