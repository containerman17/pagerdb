package pagerdb

import (
	"bytes"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"io"
	"os"
	"runtime"
	"strings"
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

func TestSealMemoryMeasure(t *testing.T) {
	for _, broad := range []bool{false, true} {
		for _, writes := range []int{8192, 65536} {
			db, e := Open(t.TempDir(), Options{MaxLeaves: 1 << 17, EpochVersions: 1 << 30, EpochBytes: 1 << 40})
			if e != nil {
				t.Fatal(e)
			}
			size := 4096
			if broad {
				size = writes * LeafSize
			}
			r, e := db.Alloc(size)
			if e != nil {
				t.Fatal(e)
			}
			var b [64]byte
			for i := 0; i < writes; i++ {
				b[0] = byte(i)
				off := 0
				if broad {
					off = i * LeafSize
				}
				db.Write(r, off, b[:])
			}
			if _, _, e = db.Commit(); e != nil {
				t.Fatal(e)
			}
			runtime.GC()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			base := m.HeapAlloc
			total := m.TotalAlloc
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
			runtime.ReadMemStats(&m)
			t.Logf("broad=%t writes=%d log_payload=%d peak_extra_heap=%d allocated=%d elapsed=%s", broad, writes, writes*128, peak.Load()-base, m.TotalAlloc-total, time.Since(start))
			db.Close()
		}
	}
}

func TestSealedRootReturnsNewest(t *testing.T) {
	db, e := Open(t.TempDir(), Options{MaxLeaves: 1 << 16, RetainVersions: 1})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	r, e := db.Alloc(4096)
	if e != nil {
		t.Fatal(e)
	}
	db.Write(r, 0, []byte{9})
	var root [32]byte
	var version uint64
	for i := 0; i < 5; i++ {
		root, version, e = db.Commit()
		if e != nil {
			t.Fatal(e)
		}
	}
	if e = db.Seal(); e != nil {
		t.Fatal(e)
	}
	got, ok, e := db.sealed[0].findRoot(root)
	if e != nil || !ok || got != version {
		t.Fatalf("got %d %t %v want %d", got, ok, e, version)
	}
	v, e := db.ViewRoot(root)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	if v.Version() != version {
		t.Fatalf("got version %d want %d", v.Version(), version)
	}
}

func xorFrameSeed() []byte {
	h := make([]byte, frameHdr)
	for _, at := range []int{4, 8} {
		le.PutUint32(h[at:], 9)
	}
	le.PutUint32(h[12:], 1)
	le.PutUint32(h[24:], 1)
	le.PutUint32(h[28:], 1)
	return append(h, 1, 0, 0, 0, 0, 0, 1, 0, 3)
}
func TestXORFrameValidation(t *testing.T) {
	seed := xorFrameSeed()
	raw := seed[frameHdr:]
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	defer enc.Close()
	for _, compress := range []bool{false, true} {
		h := append([]byte(nil), seed[:frameHdr]...)
		body := raw
		if compress {
			body = enc.EncodeAll(raw, nil)
			h[0] = 1
			le.PutUint32(h[4:], uint32(len(body)))
		}
		decoded, e := decodeXORFrame(h, body)
		if e != nil {
			t.Fatal(e)
		}
		out := []byte{2}
		if e = applyXORRecords(decoded, 1, 0, 0, out); e != nil || out[0] != 1 {
			t.Fatalf("%v %v", out, e)
		}
	}
	for _, mutate := range []func([]byte){func(b []byte) { b[0] = 4 }, func(b []byte) { b[8] = 255 }, func(b []byte) { b[12] = 2 }, func(b []byte) { b[24] = 2 }, func(b []byte) { b[len(b)-1] = 0 }, func(b []byte) { b[0] = 2 }} {
		b := append([]byte(nil), seed...)
		mutate(b)
		if _, e := decodeXORFrame(b[:frameHdr], b[frameHdr:]); e == nil {
			t.Fatal("bad frame accepted")
		}
	}
}
func xorCopyFrameSeed() []byte {
	const count = 254
	payload := make([]byte, 0, count*(denseRec+LeafSize)+LeafSize)
	for i := 0; i < count; i++ {
		var r [denseRec]byte
		le.PutUint32(r[:], 1)
		le.PutUint16(r[6:], LeafSize)
		payload = append(payload, r[:]...)
		for j := 0; j < LeafSize; j++ {
			payload = append(payload, 1)
		}
	}
	payload = append(payload, make([]byte, LeafSize)...)
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	body := enc.EncodeAll(payload, nil)
	enc.Close()
	h := make([]byte, frameHdr)
	h[0] = 3
	le.PutUint32(h[4:], uint32(len(body)))
	le.PutUint32(h[8:], uint32(len(payload)))
	le.PutUint32(h[12:], count)
	le.PutUint32(h[24:], 1)
	le.PutUint32(h[28:], 1)
	return append(h, body...)
}

func FuzzEpochFrame(f *testing.F) {
	f.Add(xorFrameSeed())
	f.Add(xorCopyFrameSeed())
	f.Add([]byte("truncated"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) < frameHdr {
			return
		}
		if body, e := decodeXORFrame(b[:frameHdr], b[frameHdr:]); e == nil {
			applyXORRecords(body, le.Uint32(b[12:]), 0, 0, make([]byte, LeafSize))
		}
	})
}
func epochIndexSeed() []byte {
	frame := xorFrameSeed()
	dirOff := epochHdr + len(frame)
	denseOff := dirOff + frameDirRec
	idxOff := denseOff + denseRec
	rootsOff := idxOff + idxRec
	image := make([]byte, rootsOff+2*rootRec)
	copy(image, epochMagic)
	for i, v := range []uint64{1, 0, 1, 1, uint64(idxOff), 2, uint64(rootsOff)} {
		le.PutUint64(image[8+i*8:], v)
	}
	copy(image[epochHdr:], frame)
	le.PutUint64(image[dirOff:], epochHdr)
	le.PutUint32(image[dirOff+16:], 1)
	copy(image[denseOff:], frame[frameHdr:frameHdr+denseRec])
	le.PutUint32(image[idxOff:], 7)
	le.PutUint32(image[idxOff+4:], 1)
	le.PutUint64(image[idxOff+8:], uint64(dirOff))
	le.PutUint64(image[idxOff+16:], 1)
	le.PutUint64(image[idxOff+32:], uint64(denseOff))
	le.PutUint64(image[idxOff+40:], 1)
	le.PutUint64(image[rootsOff+rootRec+32:], 1)
	return image
}
func FuzzEpochIndex(f *testing.F) {
	f.Add(epochIndexSeed())
	f.Add([]byte("truncated"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if s, e := parseEpoch(bytes.NewReader(b), int64(len(b)), 1); e == nil {
			row, ok, e := s.lookup(7)
			if e == nil && ok && le.Uint32(row[4:]) > 0 {
				s.undoLeaf(row, 0, 0, make([]byte, LeafSize))
			}
		}
	})
}
func TestXORDenseIndexValidation(t *testing.T) {
	valid := epochIndexSeed()
	if _, e := parseEpoch(bytes.NewReader(valid), int64(len(valid)), 1); e != nil {
		t.Fatal(e)
	}
	idx := int(le.Uint64(valid[40:]))
	dense := int(le.Uint64(valid[idx+32:]))
	dir := int(le.Uint64(valid[idx+8:]))
	for _, at := range []int{dense, dense + 5, dense + 7, dir + 8, dir + 16, dir + 20, idx + 24, idx + 32, idx + 40} {
		b := append([]byte(nil), valid...)
		b[at] = 255
		if _, e := parseEpoch(bytes.NewReader(b), int64(len(b)), 1); e == nil {
			t.Fatalf("invalid index byte %d accepted", at)
		}
	}
}

func TestXORTrimNoopAndSameVersionOrder(t *testing.T) {
	db, e := Open(t.TempDir(), Options{MaxLeaves: 1 << 16})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	r, e := db.Alloc(64)
	if e != nil {
		t.Fatal(e)
	}
	db.Write(r, 0, make([]byte, 64))
	db.Commit()
	value := make([]byte, 64)
	value[63] = 1
	db.Write(r, 0, value)
	db.Commit()
	value[63] = 2
	db.Write(r, 0, value)
	value[63] = 3
	db.Write(r, 0, value)
	db.Commit()
	db.Write(r, 0, value)
	db.Commit()
	if e = db.Seal(); e != nil {
		t.Fatal(e)
	}
	leaf := db.loc(r).off / LeafSize
	within := db.loc(r).off % LeafSize
	s := db.sealed[0]
	row, ok, e := s.lookup(leaf)
	if e != nil || !ok {
		t.Fatalf("%t %v", ok, e)
	}
	if le.Uint64(row[40:]) != 3 || le.Uint32(row[4:]) != 1 {
		t.Fatalf("index %v", row)
	}
	for i, want := range []uint32{3, 3, 2} {
		d, e := s.denseRecord(row, uint64(i))
		if e != nil {
			t.Fatal(e)
		}
		if le.Uint32(d[:]) != want || le.Uint16(d[4:]) != uint16(within+63) || le.Uint16(d[6:]) != 1 {
			t.Fatalf("dense %v", d)
		}
	}
	dir, e := s.frameRecord(row, 0)
	if e != nil {
		t.Fatal(e)
	}
	h, body, e := s.readFrame(dir)
	if e != nil {
		t.Fatal(e)
	}
	if h[0]&2 != 0 || len(body) != 27 {
		t.Fatalf("rare leaf has copy or excess payload: %d %d", h[0], len(body))
	}
	for i, want := range []byte{0, 1, 3, 3} {
		v, e := db.View(uint64(i + 1))
		if e != nil {
			t.Fatal(e)
		}
		var got [64]byte
		if e = v.Read(r, 0, 64, got[:]); e != nil {
			t.Fatal(e)
		}
		v.Close()
		if got[63] != want {
			t.Fatalf("version %d got %d want %d", i+1, got[63], want)
		}
	}
	// A disjoint range must succeed without reading frame payloads.
	old := s.ra
	s.ra = rejectFrameReader{ReaderAt: old, from: le.Uint64(dir[:]) + frameHdr, to: le.Uint64(dir[:]) + frameHdr + uint64(le.Uint32(h[4:]))}
	defer func() { s.ra = old }()
	out := []byte{0}
	if e = s.undoLeaf(row, 1, within, out); e != nil {
		t.Fatalf("disjoint dense fastpath: %v", e)
	}
}

type rejectFrameReader struct {
	io.ReaderAt
	from, to uint64
}

func (r rejectFrameReader) ReadAt(p []byte, off int64) (int, error) {
	if uint64(off) < r.to && uint64(off)+uint64(len(p)) > r.from {
		return 0, fmt.Errorf("unexpected frame payload read")
	}
	return r.ReaderAt.ReadAt(p, off)
}

func TestXORRunCopies(t *testing.T) {
	db, e := Open(t.TempDir(), Options{MaxLeaves: 1 << 16, EpochVersions: 1000})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	r, e := db.Alloc(LeafSize)
	if e != nil {
		t.Fatal(e)
	}
	snapshots := make([][]byte, 600)
	for i := range snapshots {
		b := make([]byte, LeafSize)
		for j := range b {
			b[j] = byte((i+j)%251 + 1)
		}
		snapshots[i] = b
		db.Write(r, 0, b)
		if _, _, e = db.Commit(); e != nil {
			t.Fatal(e)
		}
	}
	if e = db.Seal(); e != nil {
		t.Fatal(e)
	}
	s := db.sealed[0]
	row, ok, e := s.lookup(db.loc(r).off / LeafSize)
	if e != nil || !ok {
		t.Fatal(e)
	}
	if le.Uint32(row[4:]) < 3 {
		t.Fatal("missing run split")
	}
	for i := uint32(0); i < le.Uint32(row[4:]); i++ {
		d, e := s.frameRecord(row, i)
		if e != nil {
			t.Fatal(e)
		}
		h, body, e := s.readFrame(d)
		if e != nil {
			t.Fatal(e)
		}
		copyFlag := h[0]&2 != 0
		if copyFlag != (i+1 < le.Uint32(row[4:])) {
			t.Fatalf("copy flag on run %d", i)
		}
		if copyFlag && len(body) < LeafSize {
			t.Fatal("missing copy")
		}
	}
	for _, version := range []uint64{1, 100, 250, 254, 255, 300, 500, 599, 600} {
		v, e := db.View(version)
		if e != nil {
			t.Fatal(e)
		}
		got := make([]byte, LeafSize)
		if e = v.Read(r, 0, LeafSize, got); e != nil {
			t.Fatal(e)
		}
		v.Close()
		if !bytes.Equal(got, snapshots[version-1]) {
			t.Fatalf("bad run copy at version %d", version)
		}
	}
}

func TestXORSameVersionRunCopies(t *testing.T) {
	db, e := Open(t.TempDir(), Options{MaxLeaves: 1 << 16})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	r, e := db.Alloc(LeafSize)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = db.Commit(); e != nil {
		t.Fatal(e)
	}
	b := make([]byte, LeafSize)
	for i := 0; i < 600; i++ {
		for j := range b {
			b[j] = byte((i+j)%251 + 1)
		}
		db.Write(r, 0, b)
	}
	if _, _, e = db.Commit(); e != nil {
		t.Fatal(e)
	}
	if e = db.Seal(); e != nil {
		t.Fatal(e)
	}
	v, e := db.View(1)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	if e = v.Read(r, 0, LeafSize, b); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(b, make([]byte, LeafSize)) {
		t.Fatal("same-version run copy exposes intermediate state")
	}
}
func TestXOREpochVersionBounds(t *testing.T) {
	db := &DB{dir: t.TempDir()}
	m := make([]byte, cpRec+blockRec)
	m[0] = 'C'
	m[cpRec] = 'B'
	le.PutUint64(m[cpRec+8:], 1<<32)
	e := db.seal(&chunk{epoch: 1, m: m, tail: int64(len(m))})
	if e == nil || !strings.Contains(e.Error(), "reduce EpochVersions") {
		t.Fatalf("invalid span: %v", e)
	}
}

func TestXOROverlappingTouchesAndWrites(t *testing.T) {
	dir := t.TempDir()
	options := Options{MaxLeaves: 1 << 16, RetainVersions: 1}
	db, e := Open(dir, options)
	if e != nil {
		t.Fatal(e)
	}
	r, e := db.Alloc(2048)
	if e != nil {
		t.Fatal(e)
	}
	mapped, e := db.Map(r)
	if e != nil {
		t.Fatal(e)
	}
	db.Write(r, 0, bytes.Repeat([]byte{16}, 2048))
	if _, _, e = db.Commit(); e != nil {
		t.Fatal(e)
	}
	snapshots := [][]byte{append([]byte(nil), mapped...)}
	db.Touch(r, 0, 1024)
	mapped[0] = 1
	db.Write(r, 0, []byte{2, 3})
	mapped[0] = 9
	db.Touch(r, 0, 1024)
	mapped[1] = 7
	db.Write(r, 1, []byte{8})
	mapped[2] = 4
	if _, _, e = db.Commit(); e != nil {
		t.Fatal(e)
	}
	snapshots = append(snapshots, append([]byte(nil), mapped...))
	if e = db.Seal(); e != nil {
		t.Fatal(e)
	}
	db.Touch(r, 0, 2048)
	mapped[0] = 42
	mapped[1024] = 23
	db.Touch(r, 1024, 512)
	db.Write(r, 1024, []byte{24})
	mapped[1025] = 99
	if _, _, e = db.Commit(); e != nil {
		t.Fatal(e)
	}
	snapshots = append(snapshots, append([]byte(nil), mapped...))
	if e = db.Seal(); e != nil {
		t.Fatal(e)
	}
	check := func() {
		for i, want := range snapshots {
			v, e := db.View(uint64(i + 1))
			if e != nil {
				t.Fatal(e)
			}
			got := make([]byte, len(want))
			e = v.Read(r, 0, len(got), got)
			v.Close()
			if e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("Touch/Write history differs at version %d", i+1)
			}
		}
	}
	check()
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	db, e = Open(dir, options)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	check()
}
