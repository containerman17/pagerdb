package pagerdb

import (
	"crypto/sha256"
	"fmt"
)

// Proof is a leaf of the head state and the sibling hashes from the leaf up to the root.
type Proof struct {
	Leaf     uint64 // leaf index = state file offset / 1 KiB
	Data     [LeafSize]byte
	Siblings [][32]byte
}

// Proof returns the proof for the leaf holding byte off of region r at the committed head.
func (db *DB) Proof(r Region, off int) (Proof, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.dirty) > 0 {
		return Proof{}, ErrPending
	}
	l, err := locate(db.headU64, db.regTab, db.maxRegions, r)
	if err != nil {
		return Proof{}, err
	}
	if off < 0 || uint64(off) >= l.size {
		return Proof{}, fmt.Errorf("pagerdb: offset %d outside region of %d bytes", off, l.size)
	}
	var p Proof
	l.each(uint64(off), 1, func(fo, _, _ uint64) { p.Leaf = fo / LeafSize })
	copy(p.Data[:], db.state[p.Leaf*LeafSize:])
	i := db.maxLeaves + p.Leaf
	for level := 0; level < db.depth; level++ {
		p.Siblings = append(p.Siblings, [32]byte(db.node(i^1, level)))
		i >>= 1
	}
	return p, nil
}

// Verify checks a proof against a root.
func Verify(root [32]byte, p Proof) bool {
	if len(p.Siblings) >= 64 || p.Leaf>>len(p.Siblings) != 0 {
		return false
	}
	h := sha256.Sum256(p.Data[:])
	for i, s := range p.Siblings {
		if p.Leaf>>i&1 == 0 {
			h = hashPair(h[:], s[:])
		} else {
			h = hashPair(s[:], h[:])
		}
	}
	return h == root
}
