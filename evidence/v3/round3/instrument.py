#!/usr/bin/env python3
"""Copy Go sources to a scratch directory and add per-block EVM diagnostics.

Usage: python3 instrument.py SOURCE SCRATCH [REVISION]
Run the resulting benchmark under the shared benchmark lock on macOS.
The source checkout is never modified. No module replace is used.
"""
import pathlib
import subprocess
import sys

source, dest = map(pathlib.Path, sys.argv[1:3])
dest.mkdir(parents=True, exist_ok=False)
files = (subprocess.check_output(['git', '-C', str(source), 'ls-tree', '-r', '--name-only', sys.argv[3]], text=True).splitlines()
         if len(sys.argv) == 4 else [p.name for p in source.iterdir() if p.is_file()])
for name in files:
    if '/' not in name and (name.endswith('.go') or name in ('go.mod', 'go.sum')):
        data = (subprocess.check_output(['git', '-C', str(source), 'show', sys.argv[3] + ':' + name])
                if len(sys.argv) == 4 else (source / name).read_bytes())
        (dest / name).write_bytes(data)

def edit(name, old, new):
    p = dest / name
    s = p.read_text()
    if old not in s:
        raise SystemExit(f'missing anchor in {name}: {old}')
    p.write_text(s.replace(old, new))

edit('pagerdb.go', '"sync"', '"sync"\n"sync/atomic"\n"time"')
edit('pagerdb.go', 'type snapshotSlot struct {', 'type snapshotSlot struct {\n busy atomic.Bool')
edit('pagerdb.go', 'type DB struct {', 'type DB struct {\n diag diagnostic')
edit('pagerdb.go', 'slot.done.Wait()', '''if slot.busy.Load() { db.diag.ringPending++ }
 db.diag.wait(0, &slot.done)''')
edit('pagerdb.go', 'slot.done.Add(1)', 'slot.busy.Store(true)\nslot.done.Add(1)')
edit('disk.go', 'slot.done.Done()', 'slot.busy.Store(false)\nslot.done.Done()')
edit('pagerdb.go', 'db.latest.ready.Wait()', 'db.diag.wait(1, &db.latest.ready)')
edit('pagerdb.go', 'b.ready.Wait()', 'db.diag.wait(2, &b.ready)')
edit('pagerdb.go', 'b.persisted.Wait()', 'db.diag.wait(3, &b.persisted)')
edit('pagerdb.go', 'old.ready.Wait()', 'db.diag.wait(2, &old.ready)')
edit('pagerdb.go', 'db.disk.failure()', 'db.diagnosticFailure()')
edit('pagerdb.go', 'db.pool.each(len(slot.ids), db.copyLeaf)', '''copyStart := time.Now()
 db.pool.each(len(slot.ids), db.copyLeaf)
 db.diag.add(5, time.Since(copyStart))''')
edit('pagerdb.go', 'db.jobs <- b', '''submitStart := time.Now()
 db.jobs <- b
 db.diag.add(6, time.Since(submitStart))''')
growth_anchor = ('needed := len(db.undo) + 16 + int(n)'
                 if 'needed := len(db.undo)' in (dest / 'pagerdb.go').read_text()
                 else 'db.undo = le.AppendUint64(db.undo, off)')
edit('pagerdb.go', growth_anchor, """var growStart time.Time
 if uint64(len(db.undo))+n+16 > uint64(cap(db.undo)) { growStart = time.Now() }
 """ + growth_anchor)
edit('pagerdb.go', 'db.markDirty(off, n)', '''db.markDirty(off, n)
 if !growStart.IsZero() {
 db.diag.grows++
 db.diag.growTime += time.Since(growStart)
 }''')
(dest / 'diagnostic.go').write_text('''package pagerdb
import ("sync"; "time")
type diagnostic struct {
 calls, slow [7]int
 elapsed [7]time.Duration
 ringPending, grows int
 growTime time.Duration
}
func (d *diagnostic) add(i int, elapsed time.Duration) {
 d.calls[i]++; d.elapsed[i] += elapsed
 // A slow-call count, not an exact scheduler blocking count.
 if elapsed >= time.Microsecond { d.slow[i]++ }
}
func (d *diagnostic) wait(i int, wg *sync.WaitGroup) {
 start := time.Now(); wg.Wait(); d.add(i, time.Since(start))
}
func (db *DB) diagnosticFailure() error {
 start := time.Now(); err := db.disk.failure(); db.diag.add(4, time.Since(start)); return err
}
''')
edit('evm_bench_test.go', '"os"', '"os"\n"runtime/metrics"\n"strings"')
edit('evm_bench_test.go', 'var dirty int', '''var dirty int
 samples := []metrics.Sample{{Name:"/gc/cycles/total:gc-cycles"}, {Name:"/cpu/classes/gc/mark/assist:cpu-seconds"}}
 metrics.Read(samples)
 lastGC, lastAssist := samples[0].Value.Uint64(), samples[1].Value.Float64()
 var diagnosticRows strings.Builder
 diagnosticRows.Grow(b.N*400)''')
edit('evm_bench_test.go', 'begin := time.Now()', 'db.diag = diagnostic{}\nbegin := time.Now()')
edit('evm_bench_test.go', 'times[block] = time.Since(commit).Seconds() * 1000', '''times[block] = time.Since(commit).Seconds() * 1000
 metrics.Read(samples)
 gc, assist := samples[0].Value.Uint64(), samples[1].Value.Float64()
 d := &db.diag
 fmt.Fprintf(&diagnosticRows, "block=%d write_ms=%.6f commit_ms=%.6f grows=%d grow_ms=%.6f undo_cap=%d gc=%d assist_ms=%.6f ring_pending=%d",
 block, writes[block], times[block], d.grows, float64(d.growTime)/1e6, cap(db.latest.undo), gc-lastGC, (assist-lastAssist)*1000, d.ringPending)
 for i, name := range []string{"ring", "hash", "meta_hash", "meta_disk", "failure", "copy", "submit"} {
 fmt.Fprintf(&diagnosticRows, " %s_calls=%d %s_slow=%d %s_ms=%.6f", name, d.calls[i], name, d.slow[i], name, float64(d.elapsed[i])/1e6)
 }
 diagnosticRows.WriteByte('\\n')
 lastGC, lastAssist = gc, assist''')
edit('evm_bench_test.go', 'b.StopTimer()', 'b.StopTimer()\nfmt.Print(diagnosticRows.String())')
subprocess.run(['gofmt', '-w', str(dest)], check=True)
