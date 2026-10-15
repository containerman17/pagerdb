package pagerdb

import (
	"math/bits"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestMemoryFootprint fills PAGERDB_MEM_GIB GiB of state, commits it, waits for
// the durable image and prints process memory. It uses only the public API, so
// the same file measures older commits. Linux reads /proc/self/status; macOS
// runs vmmap and footprint against this process.
func TestMemoryFootprint(t *testing.T) {
	gib, _ := strconv.Atoi(os.Getenv("PAGERDB_MEM_GIB"))
	if gib <= 0 {
		t.Skip("set PAGERDB_MEM_GIB")
	}
	size := gib << 30
	need := uint64(size+32<<20) / LeafSize
	db, err := Open(t.TempDir(), Options{MaxLeaves: uint64(1) << bits.Len64(need-1), RetainVersions: 2})
	must(t, err)
	defer db.Close()
	r := allocTest(t, db, size)
	buf := make([]byte, 16<<20)
	start := time.Now()
	for off := 0; off < size; off += len(buf) {
		for i := range buf {
			buf[i] = byte(off>>20 + i | 1)
		}
		db.Write(r, off, buf)
		_, err := db.Commit()
		must(t, err)
	}
	must(t, db.Flush())
	fill := time.Since(start)
	must(t, db.Checkpoint())
	t.Logf("MEM state_gib=%d fill_s=%.1f checkpoint_s=%.1f", gib, fill.Seconds(), time.Since(start).Seconds()-fill.Seconds())
	runtime.GC()
	if b, err := os.ReadFile("/proc/self/status"); err == nil {
		for line := range strings.Lines(string(b)) {
			for _, k := range []string{"VmHWM", "VmRSS", "RssAnon", "RssFile", "RssShmem"} {
				if strings.HasPrefix(line, k+":") {
					t.Logf("MEM %s", strings.TrimSpace(line))
				}
			}
		}
		return
	}
	pid := strconv.Itoa(os.Getpid())
	for _, cmd := range [][]string{{"vmmap", "--summary", pid}, {"footprint", pid}} {
		out, err := exec.Command(cmd[0], cmd[1:]...).CombinedOutput()
		t.Logf("MEM %s err=%v\n%s", cmd[0], err, out)
	}
}
