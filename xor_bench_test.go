package pagerdb

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/klauspost/compress/zstd"
)

const (
	xorWorkloadBlocks = 10001
	xorWorkloadLeaves = 16
)

type xorWorkloadRecord struct {
	version uint64
	delta   [64]byte
}

type xorWorkloadMetrics struct {
	records, untrimmed, untrimmedCompressed, trimmed, trimmedCompressed uint64
	frames, compressedFrames, fileBytes                                 uint64
}

type xorWorkload struct {
	db        *DB
	region    Region
	firstLeaf uint64
	head      uint64
	metrics   xorWorkloadMetrics
	expected  map[uint64][xorWorkloadLeaves][64]byte
}

// xorWorkloadFixture changes two adjacent uint256 values per leaf per version.
// The counter and balance use big-endian integers, with leading zero bytes.
func xorWorkloadFixture(tb testing.TB, random bool, capture bool) *xorWorkload {
	tb.Helper()
	db, err := Open(tb.TempDir(), Options{MaxLeaves: 1 << 14, EpochVersions: 100, RetainVersions: 100})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Error(err)
		}
	})
	r, err := db.Alloc(xorWorkloadLeaves * LeafSize)
	if err != nil {
		tb.Fatal(err)
	}
	f := &xorWorkload{db: db, region: r, firstLeaf: db.loc(r).off / LeafSize, expected: make(map[uint64][xorWorkloadLeaves][64]byte)}
	if _, _, err = db.Commit(); err != nil {
		tb.Fatal(err)
	}
	var previous [xorWorkloadLeaves][64]byte
	var pending [xorWorkloadLeaves][]xorWorkloadRecord
	rng := rand.New(rand.NewPCG(81, 29))
	var encoder *zstd.Encoder
	if capture {
		encoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
		if err != nil {
			tb.Fatal(err)
		}
		defer encoder.Close()
	}
	epoch, start := db.cur.epoch, db.cur.start
	flush := func() {
		if !capture {
			return
		}
		for leaf := range pending {
			records := pending[leaf]
			if len(records) == 0 {
				continue
			}
			var before, after []byte
			for i := len(records) - 1; i >= 0; i-- {
				rec := records[i]
				first, last := 0, len(rec.delta)
				for first < last && rec.delta[first] == 0 {
					first++
				}
				for last > first && rec.delta[last-1] == 0 {
					last--
				}
				if first == last {
					continue
				}
				f.metrics.records++
				before = appendXORWorkloadRecord(before, uint32(rec.version-start), 0, rec.delta[:])
				after = appendXORWorkloadRecord(after, uint32(rec.version-start), uint16(first), rec.delta[first:last])
			}
			f.metrics.untrimmed += uint64(32 + len(before))
			f.metrics.trimmed += uint64(32 + len(after))
			compressed := encoder.EncodeAll(before, nil)
			if len(compressed)*10 > len(before)*9 {
				compressed = before
			}
			f.metrics.untrimmedCompressed += uint64(32 + len(compressed))
			pending[leaf] = pending[leaf][:0]
		}
	}
	for block := 0; block < xorWorkloadBlocks; block++ {
		for leaf := range previous {
			var value [64]byte
			if random {
				for i := 0; i < len(value); i += 8 {
					binary.LittleEndian.PutUint64(value[i:], rng.Uint64())
				}
			} else {
				binary.BigEndian.PutUint64(value[24:32], 1000000+uint64(block+1)*uint64(leaf+1))
				binary.BigEndian.PutUint64(value[56:64], uint64(block+1))
			}
			if capture {
				rec := xorWorkloadRecord{version: db.Version() + 1}
				for i := range value {
					rec.delta[i] = value[i] ^ previous[leaf][i]
				}
				pending[leaf] = append(pending[leaf], rec)
			}
			db.Write(r, leaf*LeafSize, value[:])
			previous[leaf] = value
		}
		_, v, err := db.Commit()
		if err != nil {
			tb.Fatal(err)
		}
		for _, age := range []uint64{1, 100, 10000} {
			if v == xorWorkloadBlocks+1-age {
				f.expected[v] = previous
			}
		}
		if db.cur.epoch != epoch {
			flush()
			epoch, start = db.cur.epoch, db.cur.start
		}
	}
	flush()
	if err = db.Seal(); err != nil {
		tb.Fatal(err)
	}
	f.head = db.Version()
	return f
}

func appendXORWorkloadRecord(dst []byte, version uint32, off uint16, delta []byte) []byte {
	dst = le.AppendUint32(dst, version)
	dst = le.AppendUint16(dst, off)
	dst = le.AppendUint16(dst, uint16(len(delta)))
	return append(dst, delta...)
}

// inspectXORWorkload counts real frame bytes, including the 32-byte frame header.
// It excludes dense rows, frame directories, and allocation metadata frames.
func inspectXORWorkload(tb testing.TB, f *xorWorkload) {
	tb.Helper()
	var actualRaw, actualRecords uint64
	for _, s := range f.db.sealed {
		var header [64]byte
		if _, err := s.ra.ReadAt(header[:], 0); err != nil {
			tb.Fatal(err)
		}
		if string(header[:8]) != "PGEPOCH3" {
			tb.Fatal("XOR metrics require PGEPOCH3")
		}
		f.metrics.fileBytes += uint64(s.size)
		for i := uint64(0); i < s.nLeaves; i++ {
			var index [48]byte
			if _, err := s.ra.ReadAt(index[:], int64(s.indexOff+i*48)); err != nil {
				tb.Fatal(err)
			}
			leaf := uint64(le.Uint32(index[:]))
			if leaf < f.firstLeaf || leaf >= f.firstLeaf+xorWorkloadLeaves {
				continue
			}
			frames, dir := le.Uint32(index[4:]), le.Uint64(index[8:])
			for frame := uint32(0); frame < frames; frame++ {
				var entry [24]byte
				if _, err := s.ra.ReadAt(entry[:], int64(dir+uint64(frame)*24)); err != nil {
					tb.Fatal(err)
				}
				var h [32]byte
				if _, err := s.ra.ReadAt(h[:], int64(le.Uint64(entry[:]))); err != nil {
					tb.Fatal(err)
				}
				f.metrics.frames++
				if h[0]&1 != 0 {
					f.metrics.compressedFrames++
				}
				if h[0]&2 != 0 {
					tb.Fatal("workload should not fill a 256 KiB run")
				}
				f.metrics.trimmedCompressed += 32 + uint64(le.Uint32(h[4:]))
				actualRaw += 32 + uint64(le.Uint32(h[8:]))
				actualRecords += uint64(le.Uint32(h[12:]))
			}
		}
	}
	if actualRaw != f.metrics.trimmed || actualRecords != f.metrics.records {
		tb.Fatalf("frame accounting: raw %d want %d, records %d want %d", actualRaw, f.metrics.trimmed, actualRecords, f.metrics.records)
	}
}

// TestXORWorkloadMetrics reports the trimming and compression tradeoffs using
// application leaves only, plus total epoch bytes including every index/root.
func TestXORWorkloadMetrics(t *testing.T) {
	if os.Getenv("PAGERDB_XOR_METRICS") != "1" {
		t.Skip("set PAGERDB_XOR_METRICS=1 for the 10001-version workload")
	}
	for _, random := range []bool{false, true} {
		name := "EVM"
		if random {
			name = "Random"
		}
		t.Run(name, func(t *testing.T) {
			f := xorWorkloadFixture(t, random, true)
			inspectXORWorkload(t, f)
			m := f.metrics
			records := float64(m.records)
			t.Logf("records=%d frames=%d raw_before=%.4f B/record raw_after=%.4f B/record zstd_before=%.4f B/record zstd_after=%.4f B/record kept_zstd=%.4f all_epoch_bytes=%.4f B/app-record", m.records, m.frames, float64(m.untrimmed)/records, float64(m.trimmed)/records, float64(m.untrimmedCompressed)/records, float64(m.trimmedCompressed)/records, float64(m.compressedFrames)/float64(m.frames), float64(m.fileBytes)/records)
			for _, age := range []uint64{1, 100, 10000} {
				v := f.head - age
				vw, err := f.db.View(v)
				if err != nil {
					t.Fatal(err)
				}
				want := f.expected[v]
				for leaf := range want {
					var got [64]byte
					if err = vw.Read(f.region, leaf*LeafSize, 64, got[:]); err != nil {
						t.Fatal(err)
					}
					if got != want[leaf] {
						t.Fatalf("age %d leaf %d differs", age, leaf)
					}
					if err = f.db.sealedRange(f.firstLeaf+uint64(leaf), v, 0, got[:]); err != nil {
						t.Fatal(err)
					}
					if got != want[leaf] {
						t.Fatalf("sealed age %d leaf %d differs", age, leaf)
					}
				}
				vw.Close()
			}
		})
	}
}

// BenchmarkXORSealedRead bypasses the retained live log. It measures a direct
// 64-byte sealed range read, excluding View creation and region lookup.
func BenchmarkXORSealedRead(b *testing.B) {
	for _, random := range []bool{false, true} {
		name := "EVM"
		if random {
			name = "Random"
		}
		b.Run(name, func(b *testing.B) {
			f := xorWorkloadFixture(b, random, false)
			for _, age := range []struct {
				name string
				n    uint64
			}{{"Back1", 1}, {"Back100", 100}, {"Back10000", 10000}} {
				b.Run(age.name, func(b *testing.B) {
					v := f.head - age.n
					want := f.expected[v]
					var buf [64]byte
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := f.db.sealedRange(f.firstLeaf+uint64(i%xorWorkloadLeaves), v, 0, buf[:]); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					if !bytes.Equal(buf[:], want[(b.N-1)%xorWorkloadLeaves][:]) {
						b.Fatal("sealed read differs from workload snapshot")
					}
				})
			}
		})
	}
}
