package pagerdb

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// diskWrites returns bytes written to the block device named by PAGERDB_DISK
// (for example nvme1n1), or -1 when unset or without /proc/diskstats.
func diskWrites() int64 {
	dev := os.Getenv("PAGERDB_DISK")
	b, err := os.ReadFile("/proc/diskstats")
	if dev == "" || err != nil {
		return -1
	}
	for line := range strings.Lines(string(b)) {
		if f := strings.Fields(line); len(f) > 9 && f[2] == dev {
			v, _ := strconv.ParseInt(f[9], 10, 64)
			return v * 512
		}
	}
	return -1
}

// peakAnon samples RssAnon every 10 ms. The returned function stops sampling and
// returns the peak in KiB, or -1 without /proc/self/status.
func peakAnon() func() int64 {
	read := func() int64 {
		b, err := os.ReadFile("/proc/self/status")
		if err != nil {
			return -1
		}
		for line := range strings.Lines(string(b)) {
			if f := strings.Fields(line); len(f) == 3 && f[0] == "RssAnon:" {
				v, _ := strconv.ParseInt(f[1], 10, 64)
				return v
			}
		}
		return -1
	}
	var mu sync.Mutex
	peak, done := read(), make(chan struct{})
	go func() {
		for t := time.NewTicker(10 * time.Millisecond); ; {
			select {
			case <-done:
				t.Stop()
				return
			case <-t.C:
				v := read()
				mu.Lock()
				peak = max(peak, v)
				mu.Unlock()
			}
		}
	}()
	return func() int64 {
		close(done)
		mu.Lock()
		defer mu.Unlock()
		return max(peak, read())
	}
}

// lagMeter tracks how far the published image trails the committed head.
type lagMeter struct {
	versions, last uint64
	bytes          float64
	written0       int64
}

func (m *lagMeter) start(db *DB) {
	db.disk.mu.Lock()
	m.written0 = db.disk.written
	db.disk.mu.Unlock()
}
func (m *lagMeter) sample(db *DB) {
	v, b := db.disk.applyState()
	m.last = db.Version() - min(v, db.Version())
	m.versions = max(m.versions, m.last)
	m.bytes = max(m.bytes, float64(b))
}

// journal returns journal bytes appended since start.
func (m *lagMeter) journal(db *DB) float64 {
	db.disk.mu.Lock()
	defer db.disk.mu.Unlock()
	return float64(db.disk.written - m.written0)
}
