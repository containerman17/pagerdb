package pagerdb

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// evmSlots uses fixed 64-byte entries with a 32-byte key followed by a
// 32-byte value. Values are uint256 counters, incremented after every read.
func evmSlots(t testing.TB, slots int, retain uint64) (*DB, Region, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, Options{RetainVersions: retain})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	r, err := db.Alloc(slots * 64)
	if err != nil {
		t.Fatal(err)
	}
	// Batch prefill through Write to avoid two million setup log records.
	buf := make([]byte, 64*1024)
	for first := 0; first < slots; {
		n := min(len(buf)/64, slots-first)
		clear(buf)
		for i := 0; i < n; i++ {
			binary.BigEndian.PutUint64(buf[i*64+24:], uint64(first+i+1))
			binary.BigEndian.PutUint64(buf[i*64+56:], 1)
		}
		db.Write(r, first*64, buf[:n*64])
		first += n
	}
	if _, _, err := db.Commit(); err != nil {
		t.Fatal(err)
	}
	// Finish setup history before measuring application blocks.
	if err := db.Seal(); err != nil {
		t.Fatal(err)
	}
	return db, r, dir
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
	db.Read(r, slot*64+32, len(value), value[:])
	for i := len(value) - 1; i >= 0; i-- {
		value[i]++
		if value[i] != 0 {
			break
		}
	}
	db.Write(r, slot*64+32, value[:])
}

func evmDisk(t testing.TB, dir string) (int64, int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var bytes int64
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		bytes += info.Size()
	}
	return bytes, len(entries)
}

// evmVMStat optionally samples macOS host counters outside the timed interval.
func evmVMStat(b *testing.B) map[string]float64 {
	b.Helper()
	if os.Getenv("PAGERDB_EVM_VMSTAT") != "1" {
		return nil
	}
	out, err := exec.Command("vm_stat").Output()
	if err != nil {
		b.Fatal(err)
	}
	counters := make(map[string]float64)
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		name := strings.TrimSuffix(fields[0], ":")
		switch name {
		case "Pageins", "Pageouts", "Swapins", "Swapouts":
			value, err := strconv.ParseFloat(strings.TrimSuffix(fields[1], "."), 64)
			if err != nil {
				b.Fatal(err)
			}
			counters[name] = value
		}
	}
	if len(counters) != 4 {
		b.Fatal("vm_stat counters missing")
	}
	return counters
}

// BenchmarkEVM uses default database options, including the
// 1 GiB / 10000-version epoch thresholds. PAGERDB_EVM_RETAIN overrides
// RetainVersions (default 4096). One iteration is one block.
// Run -benchtime=700x -cpu=8 -timeout=60m to cross three automatic seals.
// Throughput includes generation, counting, read/write, Commit and final Seal.
// Disk metrics are net logical file growth per block, including retained logs;
// they are not physical allocated bytes or device write traffic. RSS is sampled
// after Seal, before Close, and includes the whole benchmark process.
// PAGERDB_EVM_VMSTAT=1 reports host paging deltas on macOS, sampled outside
// the timer. Nonzero swap deltas identify runs affected by memory pressure.
// PAGERDB_EVM_HEAP_PROFILE writes a post-GC heap profile after metrics; select
// one distribution because each invocation overwrites the requested path.
func BenchmarkEVM(b *testing.B) {
	const slots, operations = 2_000_000, 50_000
	retain := uint64(4096)
	if value := os.Getenv("PAGERDB_EVM_RETAIN"); value != "" {
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil || n == 0 {
			b.Fatal("PAGERDB_EVM_RETAIN must be a positive integer")
		}
		retain = n
	}
	for _, sigma := range []float64{0, 20_000, 2_000} {
		name := "uniform"
		if sigma != 0 {
			name = fmt.Sprintf("hot_sigma_%g", sigma)
		}
		b.Run(name, func(b *testing.B) {
			db, r, dir := evmSlots(b, slots, retain)
			logBefore, _ := evmDisk(b, filepath.Join(dir, "log"))
			epochBefore, countBefore := evmDisk(b, filepath.Join(dir, "epochs"))
			rng := rand.New(rand.NewSource(1))
			seenSlots := make([]int, slots)
			seenLeaves := make([]int, (slots+15)/16)
			blockTimes := make([]float64, b.N)
			writeTimes := make([]float64, b.N)
			commitTimes := make([]float64, b.N)
			var unique, dirty int64
			var commitTotal time.Duration
			vmBefore := evmVMStat(b)
			b.ResetTimer()
			start := time.Now()
			for block := 0; block < b.N; block++ {
				begin := time.Now()
				for range operations {
					slot := evmSlot(rng, slots, sigma)
					if seenSlots[slot] != block+1 {
						unique++
						seenSlots[slot] = block + 1
					}
					// Entries and large-region segments align with 1 KiB leaves.
					leaf := slot / 16
					if seenLeaves[leaf] != block+1 {
						dirty++
						seenLeaves[leaf] = block + 1
					}
					evmWrite(db, r, slot)
				}
				commit := time.Now()
				if _, _, err := db.Commit(); err != nil {
					b.Fatal(err)
				}
				end := time.Now()
				commitTotal += end.Sub(commit)
				writeTimes[block] = commit.Sub(begin).Seconds() * 1000
				commitTimes[block] = end.Sub(commit).Seconds() * 1000
				blockTimes[block] = end.Sub(begin).Seconds() * 1000
			}
			drain := time.Now()
			if err := db.Seal(); err != nil {
				b.Fatal(err)
			}
			elapsed := time.Since(start)
			drainTime := time.Since(drain)
			b.StopTimer()
			vmAfter := evmVMStat(b)
			for name, after := range vmAfter {
				b.ReportMetric(after-vmBefore[name], "host-"+name+"-pages")
			}
			logAfter, _ := evmDisk(b, filepath.Join(dir, "log"))
			epochAfter, countAfter := evmDisk(b, filepath.Join(dir, "epochs"))
			out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
			if err != nil {
				b.Fatal(err)
			}
			rss, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(rss)*1024, "RSS-B")
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			b.ReportMetric(float64(mem.HeapAlloc), "Go-HeapAlloc-B")
			b.ReportMetric(float64(mem.HeapInuse), "Go-HeapInuse-B")
			b.ReportMetric(float64(mem.HeapIdle), "Go-HeapIdle-B")
			b.ReportMetric(float64(mem.HeapReleased), "Go-HeapReleased-B")
			b.ReportMetric(float64(retain), "retained-versions")
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "blocks/s")
			b.ReportMetric(float64(b.N)*operations/elapsed.Seconds(), "writes/s")
			b.ReportMetric(float64(unique)/float64(b.N), "unique-slots/block")
			b.ReportMetric(float64(dirty)/float64(b.N), "dirty-leaves/block")
			b.ReportMetric(float64(dirty)/commitTotal.Seconds(), "dirty-leaves/commit-s")
			b.ReportMetric(float64(logAfter-logBefore)/float64(b.N), "live-log-B/block")
			b.ReportMetric(float64(epochAfter-epochBefore)/float64(b.N), "epoch-B/block")
			b.ReportMetric(float64(countAfter-countBefore), "sealed-epochs")
			b.ReportMetric(drainTime.Seconds()*1000, "drain-ms")
			for _, series := range []struct {
				name   string
				values []float64
			}{
				{"block", blockTimes}, {"write", writeTimes}, {"commit", commitTimes},
			} {
				slices.Sort(series.values)
				for _, p := range []int{50, 99} {
					b.ReportMetric(series.values[int(math.Ceil(float64(b.N)*float64(p)/100))-1], fmt.Sprintf("p%d-%s-ms", p, series.name))
				}
			}
			if path := os.Getenv("PAGERDB_EVM_HEAP_PROFILE"); path != "" {
				runtime.GC()
				runtime.ReadMemStats(&mem)
				b.ReportMetric(float64(mem.HeapAlloc), "Go-postGC-HeapAlloc-B")
				f, err := os.Create(path)
				if err != nil {
					b.Fatal(err)
				}
				if err := pprof.WriteHeapProfile(f); err != nil {
					f.Close()
					b.Fatal(err)
				}
				if err := f.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestEVMSameSeedRoot(t *testing.T) {
	for _, sigma := range []float64{0, 20, 2} {
		t.Run(fmt.Sprintf("sigma_%g", sigma), func(t *testing.T) {
			run := func() [32]byte {
				db, r, _ := evmSlots(t, 2000, 0)
				rng := rand.New(rand.NewSource(1))
				for range 3 {
					for range 500 {
						evmWrite(db, r, evmSlot(rng, 2000, sigma))
					}
					if _, _, err := db.Commit(); err != nil {
						t.Fatal(err)
					}
				}
				return db.Root()
			}
			if run() != run() {
				t.Fatal("same seed produced different roots")
			}
		})
	}
}
