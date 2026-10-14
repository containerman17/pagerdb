package pagerdb

import (
	"errors"
	"os"
	"testing"
)

func TestUserfaultfd(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	db.Write(r, 7, []byte("fault"))
	_, v, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	vw, _ := db.View(v)
	defer vw.Close()
	l, err := vw.loc(r, false)
	if err != nil {
		t.Fatal(err)
	}
	m, err := vw.faultMap(r, l)
	if errors.Is(err, errFaultUnavailable) {
		if os.Getenv("PAGERDB_REQUIRE_UFFD") != "" {
			t.Fatal(err)
		}
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	vw.mappings = append(vw.mappings, m)
	if string(m.data[7:12]) != "fault" {
		t.Fatal("fault handler returned wrong page")
	}
	m.data[7] = 'x'
	var b [5]byte
	db.Read(r, 7, 5, b[:])
	if string(b[:]) != "fault" {
		t.Fatal("fault map write reached DB")
	}
	t.Logf("userfaultfd active; OS page size %d", os.Getpagesize())
}
