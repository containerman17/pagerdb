package pagerdb

import (
	"bytes"
	"encoding/binary"
	"github.com/klauspost/compress/zstd"
	"io"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiskSort(t *testing.T) {
	dir := t.TempDir()
	f, e := os.CreateTemp(dir, "input")
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	for i := 10000; i >= 0; i-- {
		var r [40]byte
		le.PutUint64(r[:], uint64(i%17))
		le.PutUint64(r[8:], uint64(i))
		f.Write(r[:])
	}
	out, e := diskSort(dir, f, 40, pieceLess)
	if e != nil {
		t.Fatal(e)
	}
	defer out.Close()
	var previous, rec [40]byte
	for i := 0; i <= 10000; i++ {
		if _, e = io.ReadFull(out, rec[:]); e != nil {
			t.Fatal(e)
		}
		if i > 0 && pieceLess(rec[:], previous[:]) {
			t.Fatal("unsorted")
		}
		previous = rec
	}
}

func TestEpochFrameValidation(t *testing.T) {
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	defer enc.Close()
	payload := make([]byte, LeafSize)
	payload = append(payload, 0, 0, 0, 1, 0, 42)
	for _, compressed := range []bool{false, true} {
		body := payload
		var h [frameHdr]byte
		if compressed {
			body = enc.EncodeAll(payload, nil)
			h[0] = 1
		}
		le.PutUint32(h[4:], uint32(len(body)))
		le.PutUint64(h[8:], 5)
		var out [LeafSize]byte
		if e := decodeFrame(h[:], body, 5, out[:]); e != nil {
			t.Fatal(e)
		}
		if out[0] != 42 {
			t.Fatal(out[0])
		}
		h[0] = 2
		if decodeFrame(h[:], body, 5, out[:]) == nil {
			t.Fatal("bad flag accepted")
		}
	}
	for _, suffix := range [][]byte{{0x80}, {0, 255, 255, 1, 0, 1}, {0, 0, 0, 255, 255}, {0, 0, 0, 0, 0}} {
		b := append(make([]byte, LeafSize), suffix...)
		var h [frameHdr]byte
		le.PutUint32(h[4:], uint32(len(b)))
		if decodeFrame(h[:], b, 0, make([]byte, LeafSize)) == nil {
			t.Fatal("malformed entry accepted")
		}
	}
}

func FuzzEpochFrame(f *testing.F) {
	h := make([]byte, frameHdr)
	le.PutUint32(h[4:], LeafSize)
	f.Add(append(h, make([]byte, LeafSize)...))
	f.Add([]byte("bad frame"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) < frameHdr {
			return
		}
		decodeFrame(b[:frameHdr], b[frameHdr:], 100, make([]byte, LeafSize))
	})
}
func FuzzEpochIndex(f *testing.F) {
	h := make([]byte, epochHdr)
	copy(h, epochMagic)
	le.PutUint64(h[8:], 1)
	le.PutUint64(h[40:], epochHdr)
	le.PutUint64(h[56:], epochHdr)
	le.PutUint64(h[48:], 1)
	h = append(h, make([]byte, rootRec)...)
	f.Add(h)
	f.Add([]byte("bad index"))
	f.Fuzz(func(t *testing.T, b []byte) {
		s, e := parseEpoch(bytes.NewReader(b), int64(len(b)), 1)
		if e == nil {
			s.lookup(0)
		}
	})
}

func TestEpochCumulativeLookup(t *testing.T) {
	db, e := Open(t.TempDir(), Options{MaxLeaves: 1 << 16, EpochVersions: 100})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	r, e := db.Alloc(4096)
	if e != nil {
		t.Fatal(e)
	}
	var roots [6][32]byte
	for i := 0; i < 6; i++ {
		if i == 0 || i == 5 {
			db.Write(r, 0, []byte{byte(i + 1)})
		}
		roots[i], _, e = db.Commit()
		if e != nil {
			t.Fatal(e)
		}
		if e = db.Seal(); e != nil {
			t.Fatal(e)
		}
	}
	for i := 0; i < 6; i++ {
		v, e := db.ViewRoot(roots[i])
		if e != nil {
			t.Fatal(e)
		}
		var b [1]byte
		if e = v.Read(r, 0, 1, b[:]); e != nil {
			t.Fatal(e)
		}
		want := byte(1)
		if i == 5 {
			want = 6
		}
		if b[0] != want {
			t.Fatalf("version %d got %d", i, b[0])
		}
		v.Close()
	}
}

func BenchmarkEpochCompression(b *testing.B) {
	for _, random := range []bool{false, true} {
		name := "EVM"
		if random {
			name = "Random"
		}
		b.Run(name, func(b *testing.B) {
			payload := make([]byte, LeafSize)
			var x uint64 = 1234567
			for i := 0; i < 1000; i++ {
				var tmp [14]byte
				n := binary.PutUvarint(tmp[:], 1)
				le.PutUint16(tmp[n:], uint16((i%16)*64))
				le.PutUint16(tmp[n+2:], 64)
				payload = append(payload, tmp[:n+4]...)
				for j := 0; j < 64; j++ {
					x ^= x << 13
					x ^= x >> 7
					x ^= x << 17
					v := byte(0)
					if random || j%32 >= 24 {
						v = byte(x)
					}
					payload = append(payload, v)
				}
			}
			enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
			defer enc.Close()
			compressed := enc.EncodeAll(payload, nil)
			body := payload
			var h [frameHdr]byte
			if len(compressed)*10 <= len(payload)*9 {
				body = compressed
				h[0] = 1
			}
			le.PutUint32(h[4:], uint32(len(body)))
			out := make([]byte, LeafSize)

			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if e := decodeFrame(h[:], body, 1000, out); e != nil {
					b.Fatal(e)
				}
			}
			b.ReportMetric(float64(len(body))/float64(len(payload)), "ratio")
		})
	}
}

func TestSealMemoryMeasure(t *testing.T) {
	for _, writes := range []int{8192, 65536} {
		db, e := Open(t.TempDir(), Options{MaxLeaves: 1 << 16, EpochVersions: 1 << 30, EpochBytes: 1 << 40})
		if e != nil {
			t.Fatal(e)
		}
		r, e := db.Alloc(4096)
		if e != nil {
			t.Fatal(e)
		}
		var b [64]byte
		for i := 0; i < writes; i++ {
			b[0] = byte(i)
			db.Write(r, 0, b[:])
		}
		if _, _, e = db.Commit(); e != nil {
			t.Fatal(e)
		}
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		base := m.HeapAlloc
		var peak atomic.Uint64
		peak.Store(base)
		done := make(chan struct{})
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					runtime.ReadMemStats(&m)
					for {
						old := peak.Load()
						if m.HeapAlloc <= old || peak.CompareAndSwap(old, m.HeapAlloc) {
							break
						}
					}
				}
			}
		}()
		start := time.Now()
		e = db.Seal()
		close(done)
		<-stopped
		if e != nil {
			t.Fatal(e)
		}
		t.Logf("writes=%d log_payload=%d peak_extra_heap=%d elapsed=%s", writes, writes*128, peak.Load()-base, time.Since(start))
		db.Close()
	}
}

func TestEpochIndexValidation(t *testing.T) {
	image := make([]byte, epochHdr+idxRec+rootRec)
	copy(image, epochMagic)
	for i, v := range []uint64{2, 0, 0, 1, epochHdr, 1, epochHdr + idxRec} {
		le.PutUint64(image[8+i*8:], v)
	}
	// A cumulative row inherited from epoch one has no local frames.
	le.PutUint64(image[epochHdr+16:], 1)
	if _, e := parseEpoch(bytes.NewReader(image), int64(len(image)), 2); e != nil {
		t.Fatal(e)
	}
	for _, at := range []int{epochHdr + 4, epochHdr + 8, epochHdr + 16, 48} {
		bad := append([]byte(nil), image...)
		bad[at] = 255
		if _, e := parseEpoch(bytes.NewReader(bad), int64(len(bad)), 2); e == nil {
			t.Fatalf("accepted mutation at %d", at)
		}
	}
}
