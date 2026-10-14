package pagerdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A separate process bounds the failure when a fault handler blocks Go GC.
func TestUserfaultfdConcurrentGC(t *testing.T) {
	if os.Getenv("PAGERDB_UFFD_GC_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestUserfaultfdConcurrentGC$", "-test.count=1", "-test.v")
		cmd.Env = append(os.Environ(), "PAGERDB_UFFD_GC_CHILD=1")
		out, err := cmd.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("userfaultfd stalled during GC: %v\n%s", ctx.Err(), out)
		}
		if err != nil {
			t.Fatalf("userfaultfd GC subprocess: %v\n%s", err, out)
		}
		if bytes.Contains(out, []byte("--- SKIP:")) {
			t.Skip(string(out))
		}
		t.Log(string(out))
		return
	}
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 15})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 8<<20)
	for off := 0; off < len(want); off += os.Getpagesize() {
		want[off] = byte(off/os.Getpagesize()%251 + 1)
		db.Write(r, off, want[off:off+1])
	}
	_, version, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	db.Write(r, 17, []byte("newer bytes"))
	if _, _, err = db.Commit(); err != nil {
		t.Fatal(err)
	}
	var views []*View
	var mappings [][]byte
	for range 4 {
		vw, e := db.View(version)
		if e != nil {
			t.Fatal(e)
		}
		l, e := vw.loc(r)
		if e != nil {
			vw.Close()
			t.Fatal(e)
		}
		m, e := vw.faultMap(r, l)
		if errors.Is(e, errFaultUnavailable) {
			vw.Close()
			if os.Getenv("PAGERDB_REQUIRE_UFFD") != "" {
				t.Fatal(e)
			}
			t.Skip(e)
		}
		if e != nil {
			vw.Close()
			t.Fatal(e)
		}
		vw.mappings = append(vw.mappings, m)
		views = append(views, vw)
		mappings = append(mappings, m.data)
	}
	defer func() {
		for _, vw := range views {
			vw.Close()
		}
	}()
	stop := make(chan struct{})
	done := make(chan struct{})
	var collections atomic.Uint64
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				runtime.GC()
				collections.Add(1)
			}
		}
	}()
	var wg sync.WaitGroup
	problems := make(chan error, len(mappings))
	for _, mapped := range mappings {
		wg.Go(func() {
			// bytes.Equal can fault in a NOSPLIT assembly routine. The native handler
			// must resolve those faults while Go is trying to stop the world.
			if !bytes.Equal(mapped, want) {
				problems <- fmt.Errorf("faulted snapshot differs")
			}
			for off := 0; off < len(mapped); off += os.Getpagesize() {
				mapped[off] ^= 1
			}
		})
	}
	wg.Wait()
	close(stop)
	<-done
	close(problems)
	for e := range problems {
		t.Error(e)
	}
	if collections.Load() == 0 {
		t.Fatal("GC did not run while mappings were faulted")
	}
	t.Logf("native userfaultfd resolved four 8 MiB mappings during %d GCs, page size %d", collections.Load(), os.Getpagesize())
}

func TestUserfaultfdKernelIO(t *testing.T) {
	if os.Getenv("PAGERDB_UFFD_IO_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestUserfaultfdKernelIO$", "-test.v")
		cmd.Env = append(os.Environ(), "PAGERDB_UFFD_IO_CHILD=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("kernel fault subprocess: %v\n%s", err, out)
		}
		if bytes.Contains(out, []byte("--- SKIP:")) {
			t.Skip(string(out))
		}
		t.Log(string(out))
		return
	}
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 14})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(32768)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte{73}, 32768)
	db.Write(r, 0, want)
	_, v, err := db.Commit()
	if err != nil {
		t.Fatal(err)
	}
	vw, err := db.View(v)
	if err != nil {
		t.Fatal(err)
	}
	defer vw.Close()
	l, err := vw.loc(r)
	if err != nil {
		t.Fatal(err)
	}
	var maps [][]byte
	for range 2 {
		m, e := vw.faultMap(r, l)
		if errors.Is(e, errFaultUnavailable) && os.Getenv("PAGERDB_REQUIRE_UFFD") == "" {
			t.Skip(e)
		}
		if e != nil {
			t.Fatal(e)
		}
		vw.mappings = append(vw.mappings, m)
		maps = append(maps, m.data)
	}
	f, err := os.CreateTemp(t.TempDir(), "io")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// The kernel reads from a mapping whose pages have never been touched.
	if n, e := f.WriteAt(maps[0], 0); e != nil || n != len(want) {
		t.Fatalf("write: %d %v", n, e)
	}
	// The kernel writes into a second mapping with no materialized pages.
	if n, e := f.ReadAt(maps[1], 0); e != nil || n != len(want) {
		t.Fatalf("read: %d %v", n, e)
	}
	if !bytes.Equal(maps[1], want) {
		t.Fatal("kernel I/O changed snapshot bytes")
	}
	maps[1][0] = 99
	got := make([]byte, len(want))
	db.Read(r, 0, len(got), got)
	if !bytes.Equal(got, want) {
		t.Fatal("private kernel I/O modified database")
	}
	t.Logf("kernel-originated userfaultfd reads and writes passed, page size %d", os.Getpagesize())
}
