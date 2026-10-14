package pagerdb

import (
	"io"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// BenchmarkSustained measures the complete write and commit loop. Use
// -benchtime=10000x for the product workload of 10000 blocks.
func BenchmarkSustained(b *testing.B) {
	db, err := Open(b.TempDir(), Options{MaxLeaves: 1 << 17, EpochVersions: 100, EpochBytes: 64 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(4 << 20)
	if err != nil {
		b.Fatal(err)
	}
	if _, _, err = db.Commit(); err != nil {
		b.Fatal(err)
	}
	times := make([]int64, b.N)
	var value [64]byte
	b.ResetTimer()
	start := time.Now()
	for block := 0; block < b.N; block++ {
		le.PutUint64(value[56:], uint64(block+1))
		for leaf := 0; leaf < 2400; leaf++ {
			db.Write(r, leaf*LeafSize, value[:])
		}
		commit := time.Now()
		if _, _, err = db.Commit(); err != nil {
			b.Fatal(err)
		}
		times[block] = time.Since(commit).Nanoseconds()
	}
	// Include completion of background sealing in the sustained throughput.
	if err = db.Seal(); err != nil {
		b.Fatal(err)
	}
	elapsed := time.Since(start)
	b.StopTimer()
	slices.Sort(times)
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "blocks/s")
	b.ReportMetric(float64(times[(len(times)-1)*50/100])/1e6, "p50-commit-ms")
	b.ReportMetric(float64(times[(len(times)-1)*99/100])/1e6, "p99-commit-ms")
}

func BenchmarkDurabilityLoop(b *testing.B) {
	for _, syncMode := range []bool{false, true} {
		name := "NoSync"
		if syncMode {
			name = "Sync"
		}
		b.Run(name, func(b *testing.B) {
			db, err := Open(b.TempDir(), Options{MaxLeaves: 1 << 14, Sync: syncMode, EpochVersions: 10000})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			r, err := db.Alloc(64 << 10)
			if err != nil {
				b.Fatal(err)
			}
			if _, _, err = db.Commit(); err != nil {
				b.Fatal(err)
			}
			var value [64]byte
			b.ResetTimer()
			for block := 0; block < b.N; block++ {
				le.PutUint64(value[56:], uint64(block+1))
				for leaf := 0; leaf < 64; leaf++ {
					db.Write(r, leaf*LeafSize, value[:])
				}
				if _, _, err = db.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type imageByteCounter int64

func (n *imageByteCounter) Write(p []byte) (int, error) {
	*n += imageByteCounter(len(p))
	return len(p), nil
}

func BenchmarkExport(b *testing.B) {
	db, err := Open(b.TempDir(), Options{MaxLeaves: 1 << 15})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(4 << 20)
	if err != nil {
		b.Fatal(err)
	}
	var value [64]byte
	value[63] = 1
	for leaf := 0; leaf < 4096; leaf++ {
		db.Write(r, leaf*LeafSize, value[:])
	}
	_, v, err := db.Commit()
	if err != nil {
		b.Fatal(err)
	}
	var n imageByteCounter
	if err = db.Export(v, &n); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(n))
	b.ResetTimer()
	for range b.N {
		if err = db.Export(v, io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkViewMapFirstTouch(b *testing.B) {
	db, err := Open(b.TempDir(), Options{MaxLeaves: 1 << 17})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(64 << 20)
	if err != nil {
		b.Fatal(err)
	}
	if _, _, err = db.Commit(); err != nil {
		b.Fatal(err)
	}
	db.Write(r, 0, []byte{1})
	if _, _, err = db.Commit(); err != nil {
		b.Fatal(err)
	}
	var setup int64
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		vw, err := db.View(1)
		if err != nil {
			b.Fatal(err)
		}
		start := time.Now()
		m, err := vw.Map(r)
		setup += time.Since(start).Nanoseconds()
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		// Read one untouched OS page, then perform a private write to that page.
		off := 32 << 20
		value := m[off]
		m[off] = value + 1
		b.StopTimer()
		vw.Close()
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(setup)/float64(b.N), "map-ns/op")
}

func processRSS(t *testing.T) int64 {
	t.Helper()
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n * 1024
}

func TestViewMapSparseRSS(t *testing.T) {
	if os.Getenv("PAGERDB_RSS_TEST") != "1" {
		t.Skip("set PAGERDB_RSS_TEST=1 for 4 GiB sparse mapping measurement")
	}
	db, err := Open(t.TempDir(), Options{MaxLeaves: 1 << 23})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := db.Alloc(4 << 30)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = db.Commit(); err != nil {
		t.Fatal(err)
	}
	db.Write(r, 0, []byte{1})
	if _, _, err = db.Commit(); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	before := processRSS(t)
	vw, err := db.View(1)
	if err != nil {
		t.Fatal(err)
	}
	defer vw.Close()
	m, err := vw.Map(r)
	if err != nil {
		t.Fatal(err)
	}
	for page := 0; page < 16; page++ {
		off := page * (256 << 20)
		if m[off] != 0 {
			t.Fatal("old view byte changed")
		}
		m[off] = 7
	}
	after := processRSS(t)
	t.Logf("OS page size %d; 4 GiB mapped, 16 pages touched; RSS before %d after %d delta %d bytes", os.Getpagesize(), before, after, after-before)
	if after-before > 256<<20 {
		t.Fatal("sparse view used more than 256 MiB extra RSS")
	}
}

// BenchmarkOpenVerified times Open on an existing, cleanly closed database.
// The payload is 4 MiB of nonzero bytes. state-B reports all allocated state
// bytes, including allocation metadata; MB/s uses that state byte count.
// Setup, root checks, and Close are outside the measured interval.
func BenchmarkOpenVerified(b *testing.B) {
	for _, syncMode := range []bool{false, true} {
		name := "NoSync"
		if syncMode {
			name = "Sync"
		}
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			options := Options{MaxLeaves: 1 << 14, Sync: syncMode}
			db, err := Open(dir, options)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if db != nil {
					db.Close()
				}
			})
			const payloadBytes = 4 << 20
			r, err := db.Alloc(payloadBytes)
			if err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, payloadBytes)
			for i := range payload {
				payload[i] = 0x6d
			}
			db.Write(r, 0, payload)
			root, version, err := db.Commit()
			if err != nil {
				b.Fatal(err)
			}
			allocated := db.hdr(hNextSeg) * SegSize
			if err = db.Close(); err != nil {
				b.Fatal(err)
			}
			db = nil
			b.SetBytes(int64(allocated))
			b.StopTimer()
			b.ResetTimer()
			for range b.N {
				b.StartTimer()
				db, err = Open(dir, options)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if db.root != root || db.version != version {
					b.Fatal("Open did not preserve the committed root and version")
				}
				err = db.Close()
				db = nil
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(payloadBytes)/(1<<20), "data-MiB")
			b.ReportMetric(float64(allocated), "state-B")
		})
	}
}
