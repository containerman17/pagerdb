package pagerdb

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestCommitVersionLimit(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, v, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	var image bytes.Buffer
	if err = db.Export(v, &image); err != nil {
		t.Fatal(err)
	}
	le.PutUint64(image.Bytes()[16:24], ^uint64(0))
	imported, err := Import(filepath.Join(t.TempDir(), "imported"), bytes.NewReader(image.Bytes()), root)
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Close()
	gotRoot, gotVersion, err := imported.Commit()
	if err == nil || gotRoot != root || gotVersion != ^uint64(0) {
		t.Fatalf("commit: root=%x version=%d error=%v", gotRoot, gotVersion, err)
	}
}
