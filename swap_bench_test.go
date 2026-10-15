package pagerdb

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

var swapSink atomic.Uint64

func swapEnv(t *testing.T, name string, def int) int {
	s := os.Getenv(name)
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		t.Fatalf("invalid %s", name)
	}
	return v
}

// swapCounters returns process major faults and global pswpin/pswpout.
// Missing /proc (non-Linux) reads as zero swap counters.
func swapCounters() (majflt, pswpin, pswpout int64) {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	majflt = int64(ru.Majflt)
	b, _ := os.ReadFile("/proc/vmstat")
	for line := range strings.Lines(string(b)) {
		f := strings.Fields(line)
		if len(f) == 2 && (f[0] == "pswpin" || f[0] == "pswpout") {
			v, _ := strconv.ParseInt(f[1], 10, 64)
			if f[0] == "pswpin" {
				pswpin = v
			} else {
				pswpout = v
			}
		}
	}
	return
}

// swapMemory reports smaps_rollup and cgroup v2 memory fields in KiB.
func swapMemory() map[string]int64 {
	m := map[string]int64{}
	if f, err := os.Open("/proc/self/smaps_rollup"); err == nil {
		s := bufio.NewScanner(f)
		for s.Scan() {
			x := strings.Fields(s.Text())
			if len(x) == 3 && slices.Contains([]string{"Rss:", "Anonymous:", "AnonHugePages:", "Swap:"}, x[0]) {
				m[strings.TrimSuffix(x[0], ":")+"KiB"], _ = strconv.ParseInt(x[1], 10, 64)
			}
		}
		f.Close()
	}
	if b, err := os.ReadFile("/proc/self/cgroup"); err == nil {
		dir := "/sys/fs/cgroup" + strings.TrimPrefix(strings.TrimSpace(string(b)), "0::")
		for _, name := range []string{"memory.current", "memory.swap.current", "memory.max"} {
			if v, err := os.ReadFile(dir + "/" + name); err == nil {
				n, e := strconv.ParseInt(strings.TrimSpace(string(v)), 10, 64)
				if e == nil {
					m["cgroup_"+name+"_KiB"] = n / 1024
				}
			}
		}
	}
	return m
}

// TestSwapEVM runs the BenchmarkEVM workload (64-byte slots, 50k
// read-increment-write ops/block) on a large state for swap experiments. It runs
// only when PAGERDB_SWAP_RUNS is set: comma-separated sigma:mode:goroutines:warmup:blocks,
// sigma as a fraction of slots (0 = uniform), mode off|pipe|inline|willneed.
// Warmup blocks run untimed before the measured blocks.
// pipe touches block k+1's leaves (state, checkpoint image, tree leaf hash) while
// block k runs and waits for it at block k+1 start; inline touches block k's
// leaves inside its timer; willneed issues MADV_WILLNEED instead of touching.
// Checkpoints are disabled; the journal still runs.
func TestSwapEVM(t *testing.T) {
	spec := os.Getenv("PAGERDB_SWAP_RUNS")
	if spec == "" {
		t.Skip("set PAGERDB_SWAP_RUNS")
	}
	const operations = 50_000
	slots := swapEnv(t, "PAGERDB_SWAP_SLOTS", 400_000_000)
	var csv *os.File
	if p := os.Getenv("PAGERDB_SWAP_CSV"); p != "" {
		f, err := os.Create(p)
		must(t, err)
		defer f.Close()
		csv = f
		fmt.Fprintln(csv, "run,block,write_ms,commit_ms,wait_ms,majflt,pswpin,pswpout")
	}
	need := (uint64(slots)*64 + 32<<20 + LeafSize - 1) / LeafSize
	maxLeaves := max(uint64(256), uint64(1)<<bits.Len64(need-1))
	db := openTest(t, Options{MaxLeaves: maxLeaves, CheckpointInterval: math.MaxUint64})

	f0, i0, o0 := swapCounters()
	start := time.Now()
	r := allocTest(t, db, slots*64)
	commitTest(t, db)
	buf := make([]byte, 16<<20) // One 16 MiB block per commit bounds undo and snapshot buffers.
	for first := 0; first < slots; {
		n := min(len(buf)/64, slots-first)
		clear(buf[:n*64])
		for i := range n {
			binary.BigEndian.PutUint64(buf[i*64+24:], uint64(first+i+1))
			buf[i*64+63] = 1
		}
		db.Write(r, first*64, buf[:n*64])
		_, err := db.Commit()
		must(t, err)
		first += n
	}
	must(t, db.Flush())
	f1, i1, o1 := swapCounters()
	t.Logf("SWAPFILL slots=%d max_leaves=%d seconds=%.1f majflt=%d pswpin=%d pswpout=%d mem=%v",
		slots, maxLeaves, time.Since(start).Seconds(), f1-f0, i1-i0, o1-o0, swapMemory())

	base := db.rangeOf(r, 0, 64)
	image, tmem, tn := db.disk.image, db.tree.mem, db.tree.n
	// Prefetch readers race benignly with writers on the same bytes; run without -race.
	launch := func(list []int, mode string, workers int, wg *sync.WaitGroup) {
		for w := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var sum byte
				for _, s := range list[len(list)*w/workers : len(list)*(w+1)/workers] {
					p := base + uint64(s)*64
					leaf := p / LeafSize
					node := (tn + leaf) * 32
					if mode == "willneed" {
						lp, np := leaf*LeafSize, node&^(LeafSize-1)
						_ = unix.Madvise(db.state[lp:lp+LeafSize], unix.MADV_WILLNEED)
						_ = unix.Madvise(image[lp:lp+LeafSize], unix.MADV_WILLNEED)
						_ = unix.Madvise(tmem[np:np+LeafSize], unix.MADV_WILLNEED)
						continue
					}
					sum += db.state[p] + image[p] + tmem[node]
				}
				swapSink.Add(uint64(sum))
			}()
		}
	}

	for run, item := range strings.Split(spec, ",") {
		parts := strings.Split(item, ":")
		if len(parts) != 5 {
			t.Fatalf("invalid run %q", item)
		}
		frac, e1 := strconv.ParseFloat(parts[0], 64)
		mode := parts[1]
		workers, e2 := strconv.Atoi(parts[2])
		warmup, e3 := strconv.Atoi(parts[3])
		blocks, e4 := strconv.Atoi(parts[4])
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || warmup < 0 || blocks < 1 || !slices.Contains([]string{"off", "pipe", "inline", "willneed"}, mode) || (mode != "off" && workers < 1) {
			t.Fatalf("invalid run %q", item)
		}
		sigma := frac * float64(slots)
		rng := rand.New(rand.NewSource(int64(run + 1))) // Distinct per run so a later run does not replay warmed slots.
		gen := func() []int {
			s := make([]int, operations)
			for i := range s {
				s[i] = evmSlot(rng, slots, sigma)
			}
			return s
		}
		var wg sync.WaitGroup
		pipelined := mode == "pipe" || mode == "willneed"
		next := gen()
		if pipelined {
			launch(next, mode, workers, &wg)
		}
		var gc0, gc1 runtime.MemStats
		runtime.ReadMemStats(&gc0)
		var writes, commits, waits []float64
		var faults, ins, outs int64
		var measured time.Time
		for block := 0; block < warmup+blocks; block++ {
			cur := next
			next = gen()
			fa, ia, oa := swapCounters()
			begin := time.Now()
			if block == warmup {
				measured = begin
			}
			switch mode {
			case "pipe", "willneed":
				wg.Wait()
				launch(next, mode, workers, &wg)
			case "inline":
				launch(cur, mode, workers, &wg)
				wg.Wait()
			}
			waited := time.Now()
			for _, s := range cur {
				evmWrite(db, r, s)
			}
			commit := time.Now()
			_, err := db.Commit()
			must(t, err)
			end := time.Now()
			fb, ib, ob := swapCounters()
			w, c, wt := commit.Sub(begin).Seconds()*1000, end.Sub(commit).Seconds()*1000, waited.Sub(begin).Seconds()*1000
			if csv != nil {
				fmt.Fprintf(csv, "%d,%d,%.3f,%.3f,%.3f,%d,%d,%d\n", run, block-warmup, w, c, wt, fb-fa, ib-ia, ob-oa)
			}
			if block >= warmup {
				writes, commits, waits = append(writes, w), append(commits, c), append(waits, wt)
				faults, ins, outs = faults+fb-fa, ins+ib-ia, outs+ob-oa
			}
		}
		wg.Wait()
		must(t, db.Flush())
		elapsed := time.Since(measured)
		runtime.ReadMemStats(&gc1)
		pct := func(v []float64, p float64) float64 {
			slices.Sort(v)
			return v[int(math.Ceil(float64(len(v))*p/100))-1]
		}
		res := map[string]any{
			"run": run, "sigma_fraction": frac, "mode": mode, "goroutines": workers,
			"warmup": warmup, "blocks": blocks, "writes_per_s": math.Round(float64(blocks*operations) / elapsed.Seconds()),
			"p50_write_ms": pct(writes, 50), "p99_write_ms": pct(writes, 99),
			"p50_commit_ms": pct(commits, 50), "p99_commit_ms": pct(commits, 99),
			"p50_wait_ms": pct(waits, 50), "p99_wait_ms": pct(waits, 99),
			"majflt_per_block":  float64(faults) / float64(blocks),
			"pswpin_per_block":  float64(ins) / float64(blocks),
			"pswpout_per_block": float64(outs) / float64(blocks),
			"gc_cycles":         gc1.NumGC - gc0.NumGC, "heap_sys_MiB": gc1.HeapSys >> 20,
			"mem": swapMemory(),
		}
		out, _ := json.Marshal(res)
		t.Logf("SWAPRESULT %s", out)
	}
}
