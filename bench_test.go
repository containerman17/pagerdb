package pagerdb

import (
	"math/rand/v2"
	"testing"
)

// benchDB opens a default-capacity store (2^24 leaves, 16 GiB) with n regions of 1 MiB.
func benchDB(b *testing.B, n int, o Options) (*DB, []Region) {
	db, err := Open(b.TempDir(), o)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	rs := make([]Region, n)
	for i := range rs {
		if rs[i], err = db.Alloc(1 << 20); err != nil {
			b.Fatal(err)
		}
	}
	if _, _, err := db.Commit(); err != nil {
		b.Fatal(err)
	}
	return db, rs
}

// history commits blocks of random 64 B writes.
func history(b *testing.B, db *DB, rs []Region, blocks, writes int) {
	rng := rand.New(rand.NewPCG(1, 2))
	var buf [64]byte
	for range blocks {
		for range writes {
			for i := range buf {
				buf[i] = byte(rng.Uint32())
			}
			db.Write(rs[rng.IntN(len(rs))], rng.IntN(1<<20/64)*64, buf[:])
		}
		if _, _, err := db.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWrite64(b *testing.B) {
	db, rs := benchDB(b, 256, Options{EpochBytes: 1 << 40, EpochVersions: 1 << 40})
	rng := rand.New(rand.NewPCG(1, 2))
	type w struct {
		r   Region
		off int
	}
	ws := make([]w, 1<<16)
	for i := range ws {
		ws[i] = w{rs[rng.IntN(len(rs))], rng.IntN(1<<20/64) * 64}
	}
	buf := make([]byte, 64)
	b.SetBytes(64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x := ws[i&(len(ws)-1)]
		buf[0] = byte(i)
		db.Write(x.r, x.off, buf)
		if i%10000 == 9999 {
			b.StopTimer()
			if _, _, err := db.Commit(); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	}
}

// BenchmarkCommit50k times Commit of a block with 50,000 distinct dirty leaves out of 1 GiB of regions.
func BenchmarkCommit50k(b *testing.B) {
	const dirty = 50000
	db, rs := benchDB(b, 1024, Options{EpochBytes: 1 << 40, EpochVersions: 1 << 40})
	rng := rand.New(rand.NewPCG(1, 2))
	buf := make([]byte, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for _, leaf := range rng.Perm(len(rs) * 1024)[:dirty] {
			buf[0] = byte(i)
			db.Write(rs[leaf/1024], leaf%1024*LeafSize+rng.IntN(LeafSize/64)*64, buf)
		}
		b.StartTimer()
		if _, _, err := db.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(dirty)*float64(b.N)/b.Elapsed().Seconds(), "leaves/s")
}

func benchViewRead(b *testing.B, sealed bool) {
	o := Options{EpochBytes: 1 << 40, EpochVersions: 1 << 40}
	if sealed {
		o.EpochVersions = 50
	}
	db, rs := benchDB(b, 64, o)
	history(b, db, rs, 200, 1000)
	if sealed {
		if err := db.Seal(); err != nil {
			b.Fatal(err)
		}
	}
	vw, err := db.View(100)
	if err != nil {
		b.Fatal(err)
	}
	defer vw.Close()
	if vw.window == sealed {
		b.Fatalf("view at 100 window=%v", vw.window)
	}
	rng := rand.New(rand.NewPCG(3, 4))
	buf := make([]byte, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := vw.Read(rs[rng.IntN(len(rs))], rng.IntN(1<<20/64)*64, 64, buf); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "reads/s")
}

// BenchmarkViewReadWindow reads 64 B at a version 100 blocks (100k writes) behind the head, from the live log.
func BenchmarkViewReadWindow(b *testing.B) { benchViewRead(b, false) }

// BenchmarkViewReadSealed reads 64 B at the same version after it was sealed into epochs of 50 versions.
func BenchmarkViewReadSealed(b *testing.B) { benchViewRead(b, true) }

// BenchmarkWrite32Resident times 32-byte accesses to a resident 128 MiB region
// at random 64-byte slots, the steady-state write path. The region is written
// once before timing, so state page faults are excluded. Commit runs every
// 50000 operations outside the timer with RetainVersions 32. Write is a bare
// Write; Read is a bare Read; ReadWrite reads the slot first, so the Write
// finds its state line in cache, as in a read-modify-write of a storage slot.
func BenchmarkWrite32Resident(b *testing.B) {
	for _, op := range []string{"Write", "Read", "ReadWrite"} {
		b.Run(op, func(b *testing.B) {
			const size, perBlock = 128 << 20, 50000
			db, err := Open(b.TempDir(), Options{RetainVersions: 32})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			r, err := db.Alloc(size)
			if err != nil {
				b.Fatal(err)
			}
			fill := make([]byte, 1<<20)
			for i := range fill {
				fill[i] = byte(i)
			}
			for off := 0; off < size; off += len(fill) {
				db.Write(r, off, fill)
			}
			if _, _, err = db.Commit(); err != nil {
				b.Fatal(err)
			}
			if err = db.Seal(); err != nil {
				b.Fatal(err)
			}
			rng := rand.New(rand.NewPCG(3, 4))
			offs := make([]int, 1<<20)
			for i := range offs {
				offs[i] = rng.IntN(size/64)*64 + 32
			}
			var value [32]byte
			b.SetBytes(32)
			b.ResetTimer()
			for i := range b.N {
				off := offs[i&(len(offs)-1)]
				if op != "Write" {
					db.Read(r, off, len(value), value[:])
				}
				if op != "Read" {
					value[0] = byte(i)
					db.Write(r, off, value[:])
				}
				if i%perBlock == perBlock-1 {
					b.StopTimer()
					if _, _, err = db.Commit(); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			}
		})
	}
}
