package pagerdb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

var crashOptions = Options{MaxLeaves: testLeaves, RetainVersions: 8, EpochBytes: 64 << 10, Workers: 4}

// crashStep runs session step i: a rewind every 17th step, otherwise one block
// of allocation changes, writes and partial undo. It depends only on seed, i and
// the current state, so a reference DB can repeat it.
func crashStep(db *DB, seed int64, i int) error {
	rng := rand.New(rand.NewSource(seed<<20 + int64(i)))
	if i%17 == 16 {
		return db.Rewind(db.Version() - uint64(1+rng.Intn(3)))
	}
	var live []Region
	var used uint64
	for r := Region(1); uint64(r) < le.Uint64(db.state[16:]); r++ {
		if _, n, err := location(db.state, r); err == nil {
			live = append(live, r)
			used += align(n)
		}
	}
	pick := func() Region { return live[rng.Intn(len(live))] }
	switch {
	case len(live) == 0 || rng.Intn(10) == 0:
		if _, err := db.Alloc(1 + rng.Intn(40000)); err != nil && !errors.Is(err, ErrFull) {
			return err
		}
	case used > 8<<20 || rng.Intn(25) == 0:
		if err := db.Free(pick()); err != nil {
			return err
		}
	case rng.Intn(15) == 0:
		r := pick()
		m, _ := db.Map(r)
		if err := db.Grow(r, len(m)+rng.Intn(20000)); err != nil && !errors.Is(err, ErrFull) {
			return err
		}
	}
	live = live[:0]
	for r := Region(1); uint64(r) < le.Uint64(db.state[16:]); r++ {
		if _, _, err := location(db.state, r); err == nil {
			live = append(live, r)
		}
	}
	if len(live) > 0 {
		mark, undo := db.Mark(), rng.Intn(4) == 0
		for range 1 + rng.Intn(60) {
			r := pick()
			m, _ := db.Map(r)
			n := min(len(m), 1+rng.Intn(300))
			if rng.Intn(20) == 0 {
				n = len(m)
			}
			off := rng.Intn(len(m) - n + 1)
			data := make([]byte, n)
			rng.Read(data)
			db.Write(r, off, data)
			if undo && rng.Intn(3) == 0 {
				db.Undo(mark)
				mark = db.Mark()
			}
		}
	}
	_, err := db.Commit()
	return err
}

// TestCrashRecovery kills a child process during a write-heavy run, either at a
// random time or frozen at a named disk step, then reopens the same directory.
// A reference DB repeats the child's steps; the recovered version, root and
// state bytes must equal the reference after some step no earlier than the last
// step the child flushed. PAGERDB_CRASH_ITERS sets the number of kills.
func TestCrashRecovery(t *testing.T) {
	if dir := os.Getenv("PAGERDB_CRASH_DIR"); dir != "" {
		crashChild(dir)
		return
	}
	iters := 8
	if s := os.Getenv("PAGERDB_CRASH_ITERS"); s != "" {
		iters, _ = strconv.Atoi(s)
	}
	dir := t.TempDir()
	ref := openTest(t, crashOptions)
	// Session zero starts from an empty store on both sides.
	points := []string{"", "apply", "applying", "image-synced", "manifest-written", "manifest-renamed", "rotated"}
	counts := map[string]int{}
	rng := rand.New(rand.NewSource(1))
	for it := 0; it < iters; it++ {
		point := points[it%len(points)]
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrashRecovery$")
		cmd.Env = append(os.Environ(), "PAGERDB_CRASH_DIR="+dir, fmt.Sprintf("PAGERDB_CRASH_SEED=%d", it),
			"PAGERDB_CRASH_POINT="+point, fmt.Sprintf("PAGERDB_CRASH_HIT=%d", 1+rng.Intn(4)),
			fmt.Sprintf("PAGERDB_CRASH_INTERVAL_MS=%d", rng.Intn(20)))
		out, err := cmd.StdoutPipe()
		must(t, err)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		must(t, cmd.Start())
		lines := make(chan string)
		go func() {
			s := bufio.NewScanner(out)
			for s.Scan() {
				lines <- s.Text()
			}
			close(lines)
		}()
		lastStep, lastFlush := -1, -1
		parse := func(line string) bool {
			f := strings.Fields(line)
			switch f[0] {
			case "S":
				lastStep, _ = strconv.Atoi(f[1])
			case "F":
				lastFlush, _ = strconv.Atoi(f[1])
			}
			return f[0] == "POINT"
		}
		deadline := time.After(time.Duration(20+rng.Intn(300)) * time.Millisecond)
		if point != "" {
			deadline = time.After(20 * time.Second)
		}
		reason := "timer"
	wait:
		for {
			select {
			case line, ok := <-lines:
				if !ok {
					cmd.Wait()
					t.Fatalf("child exited: %s", stderr.String())
				}
				if parse(line) {
					reason = point
					break wait
				}
			case <-deadline:
				break wait
			}
		}
		must(t, cmd.Process.Kill())
		// Other goroutines keep stepping until the kill lands; count those steps too.
		for line := range lines {
			parse(line)
		}
		cmd.Wait()
		counts[reason]++
		db, err := Open(dir, crashOptions)
		if err != nil {
			t.Fatalf("iteration %d (%s): reopen: %v", it, reason, err)
		}
		// Advance the reference until it matches. Step lastStep+1 may have committed
		// without printing; later steps cannot have.
		steps := 0
		for !(steps > lastFlush && ref.Version() == db.Version() && ref.Root() == db.Root()) {
			if steps > lastStep+1 {
				if keep := os.Getenv("PAGERDB_CRASH_KEEP"); keep != "" {
					exec.Command("cp", "-R", dir, keep).Run()
				}
				t.Fatalf("iteration %d (%s): recovered version %d matches no step in [%d, %d]", it, reason, db.Version(), lastFlush+1, lastStep+2)
			}
			if os.Getenv("PAGERDB_CRASH_DEBUG") != "" {
				t.Logf("ref step %d: version %d root %x (recovered %d %x)", steps, ref.Version(), ref.Root(), db.Version(), db.Root())
			}
			must(t, crashStep(ref, int64(it), steps))
			steps++
		}
		if os.Getenv("PAGERDB_CRASH_DEBUG") != "" {
			v, lagBytes := db.disk.applyState()
			t.Logf("it %d %s: flushed %d printed %d matched %d version %d image %d backlog %d", it, reason, lastFlush, lastStep, steps, db.Version(), v, lagBytes)
		}
		if !bytes.Equal(ref.state, db.state) {
			t.Fatalf("iteration %d: recovered state differs", it)
		}
		must(t, db.Verify())
		if it%3 == 0 {
			must(t, db.Checkpoint())
		}
		must(t, db.Close())
		// The reference keeps its window across sessions; the child starts each
		// session with an empty window, which crashStep's rewind rule respects.
	}
	t.Logf("crash iterations: %d %v", iters, counts)
}

func crashChild(dir string) {
	seed, _ := strconv.ParseInt(os.Getenv("PAGERDB_CRASH_SEED"), 10, 64)
	ms, _ := strconv.Atoi(os.Getenv("PAGERDB_CRASH_INTERVAL_MS"))
	applyInterval = time.Duration(ms) * time.Millisecond
	db, err := Open(dir, crashOptions)
	if err != nil {
		panic(err)
	}
	if point := os.Getenv("PAGERDB_CRASH_POINT"); point != "" {
		hit, _ := strconv.Atoi(os.Getenv("PAGERDB_CRASH_HIT"))
		hook := func(p string) {
			if p == point {
				if hit--; hit == 0 {
					fmt.Println("POINT")
					os.Stdout.Sync()
					select {}
				}
			}
		}
		db.diskHook.Store(&hook)
	}
	w := bufio.NewWriter(os.Stdout)
	for i := 0; ; i++ {
		if err := crashStep(db, seed, i); err != nil {
			panic(err)
		}
		fmt.Fprintf(w, "S %d\n", i)
		if i%3 == 0 {
			if err := db.Flush(); err != nil {
				panic(err)
			}
			fmt.Fprintf(w, "F %d\n", i)
		}
		w.Flush()
	}
}
