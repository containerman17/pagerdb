package pagerdb

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"
)

func evmSlots(t testing.TB, slots int, retain uint64) (*DB, Region) {
	t.Helper()
	db := openTest(t, Options{MaxLeaves: 256 << 20 / LeafSize, RetainVersions: retain})
	r := allocTest(t, db, slots*64)
	buf := make([]byte, 64*1024)
	for first := 0; first < slots; {
		n := min(len(buf)/64, slots-first)
		clear(buf)
		for i := 0; i < n; i++ {
			binary.BigEndian.PutUint64(buf[i*64+24:], uint64(first+i+1))
			buf[i*64+63] = 1
		}
		db.Write(r, first*64, buf[:n*64])
		first += n
	}
	commitTest(t, db)
	must(t, db.Checkpoint())
	return db, r
}
func evmSlot(rng *rand.Rand, slots int, sigma float64) int {
	if sigma == 0 {
		return rng.Intn(slots)
	}
	i := (slots/2 + int(math.Round(rng.NormFloat64()*sigma))) % slots
	if i < 0 {
		i += slots
	}
	return i
}
func evmWrite(db *DB, r Region, slot int) {
	var value [32]byte
	db.Read(r, slot*64+32, 32, value[:])
	for i := 31; i >= 0; i-- {
		value[i]++
		if value[i] != 0 {
			break
		}
	}
	db.Write(r, slot*64+32, value[:])
}

// BenchmarkEVM preserves the v2 workload: 2M 64-byte slots, uint256 counters,
// 50k writes/block, uniform or normal sigma 1% / 0.1%. Final Flush is timed.
// Commit timing measures snapshot plus prior hash wait, not root completion.
func BenchmarkEVM(b *testing.B) {
	const slots, operations = 2_000_000, 50_000
	retain := uint64(32)
	if s := os.Getenv("PAGERDB_EVM_RETAIN"); s != "" {
		v, e := strconv.ParseUint(s, 10, 64)
		if e != nil || v == 0 {
			b.Fatal("invalid PAGERDB_EVM_RETAIN")
		}
		retain = v
	}
	for _, sigma := range []float64{0, 20000, 2000} {
		name := "uniform"
		if sigma != 0 {
			name = fmt.Sprintf("hot_sigma_%g", sigma)
		}
		b.Run(name, func(b *testing.B) {
			db, r := evmSlots(b, slots, retain)
			rng := rand.New(rand.NewSource(1))
			seen := make([]int, (slots+LeafSize/64-1)/(LeafSize/64))
			times := make([]float64, b.N)
			writes := make([]float64, b.N)
			var dirty int
			var lag lagMeter
			b.ResetTimer()
			disk0, peak := diskWrites(), peakAnon()
			lag.start(db)
			start := time.Now()
			for block := 0; block < b.N; block++ {
				begin := time.Now()
				for range operations {
					slot := evmSlot(rng, slots, sigma)
					leaf := slot / (LeafSize / 64)
					if seen[leaf] != block+1 {
						seen[leaf] = block + 1
						dirty++
					}
					evmWrite(db, r, slot)
				}
				commit := time.Now()
				_, e := db.Commit()
				must(b, e)
				lag.sample(db)
				writes[block] = commit.Sub(begin).Seconds() * 1000
				times[block] = time.Since(commit).Seconds() * 1000
			}
			must(b, db.Flush())
			elapsed := time.Since(start)
			disk1, peakKiB := diskWrites(), peak()
			b.StopTimer()
			lag.sample(db)
			end := lag.last
			catchup := time.Now()
			must(b, db.Checkpoint())
			b.ReportMetric(float64(lag.versions), "max-lag-versions")
			b.ReportMetric(float64(end), "end-lag-versions")
			b.ReportMetric(lag.bytes/1e6, "max-lag-MB")
			b.ReportMetric(time.Since(catchup).Seconds()*1000, "catchup-ms")
			b.ReportMetric(lag.journal(db)/1e6/elapsed.Seconds(), "journal-MB/s")
			if disk0 >= 0 {
				b.ReportMetric(float64(disk1-disk0)/1e6/elapsed.Seconds(), "disk-MB/s")
			}
			if peakKiB >= 0 {
				b.ReportMetric(float64(peakKiB)/1024, "peak-anon-MiB")
			}
			b.ReportMetric(float64(b.N*operations)/elapsed.Seconds(), "writes/s")
			b.ReportMetric(float64(dirty)/float64(b.N), "dirty-leaves/block")
			for _, series := range []struct {
				name string
				v    []float64
			}{{"write", writes}, {"commit", times}} {
				slices.Sort(series.v)
				for _, p := range []int{50, 99} {
					b.ReportMetric(series.v[int(math.Ceil(float64(b.N)*float64(p)/100))-1], fmt.Sprintf("p%d-%s-ms", p, series.name))
				}
			}
		})
	}
}
func TestEVMSameSeedRoot(t *testing.T) {
	for _, sigma := range []float64{0, 20, 2} {
		run := func() [32]byte {
			db, r := evmSlots(t, 2000, 3)
			rng := rand.New(rand.NewSource(1))
			for range 3 {
				for range 500 {
					evmWrite(db, r, evmSlot(rng, 2000, sigma))
				}
				commitTest(t, db)
			}
			root := db.Root()
			must(t, db.Close())
			return root
		}
		if run() != run() {
			t.Fatal("same seed roots differ")
		}
	}
}
