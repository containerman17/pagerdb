package pagerdb

import (
	"crypto/sha256"
	"math/bits"
	"slices"

	"golang.org/x/sys/unix"
)

type work struct {
	n  int
	fn func(int)
}
type pool struct {
	jobs []chan work
	done chan struct{}
}

func newPool(n int) *pool {
	p := &pool{done: make(chan struct{}, n)}
	for w := 0; w < n; w++ {
		ch := make(chan work)
		p.jobs = append(p.jobs, ch)
		go func() {
			for j := range ch {
				for i := j.n * w / n; i < j.n*(w+1)/n; i++ {
					j.fn(i)
				}
				p.done <- struct{}{}
			}
		}()
	}
	return p
}
func (p *pool) each(n int, fn func(int)) {
	if n < 256 {
		for i := 0; i < n; i++ {
			fn(i)
		}
		return
	}
	for _, ch := range p.jobs {
		ch <- work{n, fn}
	}
	for range p.jobs {
		<-p.done
	}
}
func (p *pool) close() {
	for _, ch := range p.jobs {
		close(ch)
	}
}
func hashPair(a, b [32]byte) [32]byte {
	var x [64]byte
	copy(x[:32], a[:])
	copy(x[32:], b[:])
	return sha256.Sum256(x[:])
}

// Untouched virtual tree pages contain zero, interpreted as the precomputed
// empty subtree at that level. Only paths to touched leaves consume RAM.
type tree struct {
	n          uint64
	mem        []byte
	empty      [][32]byte
	nodes      []uint64
	ids        []uint32
	data       []byte
	level      int
	hashLeaf   func(int)
	hashParent func(int)
}

func newTree(n uint64) (*tree, error) {
	mem, err := anonymous(int(2 * n * 32))
	if err != nil {
		return nil, err
	}
	t := &tree{n: n, mem: mem, empty: make([][32]byte, bits.Len64(n))}
	t.hashLeaf = func(i int) { t.set(t.n+uint64(t.ids[i]), sha256.Sum256(t.data[i*LeafSize:(i+1)*LeafSize])) }
	t.hashParent = func(i int) { v := t.nodes[i]; t.set(v, hashPair(t.get(2*v, t.level-1), t.get(2*v+1, t.level-1))) }
	t.empty[0] = sha256.Sum256(make([]byte, LeafSize))
	for i := 1; i < len(t.empty); i++ {
		t.empty[i] = hashPair(t.empty[i-1], t.empty[i-1])
	}
	return t, nil
}
func (t *tree) get(i uint64, level int) [32]byte {
	h := [32]byte(t.mem[i*32 : i*32+32])
	if h == ([32]byte{}) {
		return t.empty[level]
	}
	return h
}
func (t *tree) set(i uint64, h [32]byte) { copy(t.mem[i*32:i*32+32], h[:]) }
func (t *tree) root() [32]byte           { return t.get(1, len(t.empty)-1) }
func (t *tree) update(ids []uint32, data []byte, p *pool) {
	if len(ids) == 0 {
		return
	}
	t.ids = ids
	t.data = data
	p.each(len(ids), t.hashLeaf)
	t.nodes = slices.Grow(t.nodes[:0], len(ids))[:len(ids)]
	for i, id := range ids {
		t.nodes[i] = t.n + uint64(id)
	}
	for t.level = 1; t.nodes[0] > 1; t.level++ {
		next := t.nodes[:0]
		last := uint64(0)
		for _, id := range t.nodes {
			v := id / 2
			if v != last {
				next = append(next, v)
				last = v
			}
		}
		t.nodes = next
		p.each(len(t.nodes), t.hashParent)
	}
	t.ids = nil
	t.data = nil
}
func (t *tree) close() error { return unix.Munmap(t.mem) }
