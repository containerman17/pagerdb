package pagerdb

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// workload runs deterministic random blocks; each is called after every Commit.
func workload(t testing.TB, db *DB, seed uint64, blocks int, each func(v uint64, root [32]byte, m model)) {
	rng := rand.New(rand.NewPCG(seed, seed))
	m := model{}
	for b := 0; blocks < 0 || b < blocks; b++ {
		for range rng.IntN(30) {
			step(t, rng, db, m, smallSizes)
		}
		root, v, err := db.Commit()
		if err != nil {
			t.Fatal(err)
		}
		each(v, root, m)
	}
}

// reference replays the workload without crashing and returns roots and models per version.
func reference(t *testing.T, o Options, seed uint64, blocks int) ([][32]byte, []model) {
	db, err := Open(t.TempDir(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	roots, snaps := [][32]byte{db.Root()}, []model{{}}
	workload(t, db, seed, blocks, func(_ uint64, root [32]byte, m model) {
		roots = append(roots, root)
		snaps = append(snaps, m.clone())
	})
	return roots, snaps
}

// crash drops a DB the way a killed process would: no undo, no sync, files left as they are.
func crash(db *DB) {
	db.stopSealer()
	db.mu.Lock()
	db.unmap()
	db.closeSealed()
}

var crashOpts = Options{MaxLeaves: 1 << 14, EpochVersions: 10}

// TestCrashChild is the child process of TestRecoveryKill: it writes blocks until killed.
func TestCrashChild(t *testing.T) {
	dir := os.Getenv("PAGERDB_CRASH_DIR")
	if dir == "" {
		t.Skip("child process only")
	}
	db, err := Open(dir, crashOpts)
	if err != nil {
		t.Fatal(err)
	}
	workload(t, db, 7, -1, func(v uint64, _ [32]byte, _ model) { fmt.Println(v) })
}

func TestRecoveryKill(t *testing.T) {
	const target = 40
	roots, snaps := reference(t, crashOpts, 7, target+30)
	rng := rand.New(rand.NewPCG(5, 6))
	for round := range 4 {
		dir := t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1")
		cmd.Env = append(os.Environ(), "PAGERDB_CRASH_DIR="+dir)
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(out)
		last := uint64(0)
		for sc.Scan() {
			if v, err := strconv.ParseUint(sc.Text(), 10, 64); err == nil {
				last = v
			}
			if last >= target {
				break
			}
		}
		time.Sleep(time.Duration(rng.IntN(3000)) * time.Microsecond)
		cmd.Process.Kill() // SIGKILL
		cmd.Wait()
		tail := tornTail(t, dir)
		db, err := Open(dir, crashOpts)
		if err != nil {
			t.Fatalf("round %d: reopen: %v", round, err)
		}
		v := db.Version()
		if v < last || v >= uint64(len(roots)) {
			t.Fatalf("round %d: recovered version %d, child had committed %d", round, v, last)
		}
		if db.Root() != roots[v] {
			t.Fatalf("round %d: version %d root differs from the reference", round, v)
		}
		checkModel(t, db, snaps[v])
		for _, ov := range []uint64{v, v / 2, 1} {
			vw, err := db.View(ov)
			if err != nil {
				t.Fatal(err)
			}
			checkView(t, rng, vw, snaps[ov])
			vw.Close()
		}
		t.Logf("round %d: child printed %d, recovered %d with %d torn tail entries, window %d chunks, %d sealed",
			round, last, v, tail, len(db.chunks), len(db.sealed))
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// tornTail counts the uncommitted entries at the end of the newest log chunk.
func tornTail(t *testing.T, dir string) int {
	des, err := os.ReadDir(filepath.Join(dir, "log"))
	if err != nil || len(des) == 0 {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "log", des[len(des)-1].Name()))
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, tail, _ := scan(b)
	return len(tail)
}

func TestRecoveryTornTail(t *testing.T) {
	o := Options{MaxLeaves: 1 << 14}
	const n = 20
	roots, snaps := reference(t, o, 9, n)
	for _, cut := range []string{"block record", "entry"} {
		t.Run(cut, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(dir, o)
			if err != nil {
				t.Fatal(err)
			}
			workload(t, db, 9, n, func(uint64, [32]byte, model) {})
			c := db.cur
			last := db.blocks[len(db.blocks)-1]
			prev := db.blocks[len(db.blocks)-2]
			path := db.chunkPath(c.epoch)
			crash(db)
			at := last.end - blockRec/2 // inside the last block record
			if cut == "entry" {
				at = prev.end + entryHdr + 10 // inside the first entry of the last block
			}
			if err := os.Truncate(path, at); err != nil {
				t.Fatal(err)
			}
			db, err = Open(dir, o)
			if cut == "entry" {
				// The last block's later entries are lost but their bytes are in the state:
				// recovery must refuse instead of returning a state that matches no root.
				if err == nil {
					db.Close()
					t.Fatal("open succeeded with lost log entries")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if db.Version() != n-1 || db.Root() != roots[n-1] {
				t.Fatalf("recovered version %d, want %d with its root", db.Version(), n-1)
			}
			checkModel(t, db, snaps[n-1])
			if _, _, err := db.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
