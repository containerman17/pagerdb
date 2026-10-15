package pagerdb

import (
	"crypto/sha256"
	"fmt"
)

type Proof struct {
	Leaf     uint64
	Data     [LeafSize]byte
	Siblings [][32]byte
}

// Proof proves the state leaf containing off. It does not prove the mapping
// from a region handle to that leaf; allocation metadata has its own leaves.
func (db *DB) Proof(r Region, off int) (Proof, error) {
	db.check()
	if len(db.undo) != 0 {
		return Proof{}, ErrPending
	}
	db.waitHash()
	p, n, err := location(db.state, r)
	if err != nil {
		return Proof{}, err
	}
	if off < 0 || uint64(off) >= n {
		return Proof{}, fmt.Errorf("pagerdb: proof offset out of bounds")
	}
	var proof Proof
	proof.Leaf = (p + uint64(off)) / LeafSize
	copy(proof.Data[:], db.state[proof.Leaf*LeafSize:])
	for i, level := db.o.MaxLeaves+proof.Leaf, 0; i > 1; i, level = i/2, level+1 {
		proof.Siblings = append(proof.Siblings, db.tree.get(i^1, level))
	}
	return proof, nil
}
func Verify(root [32]byte, p Proof) bool {
	if len(p.Siblings) >= 64 || p.Leaf>>len(p.Siblings) != 0 {
		return false
	}
	h := sha256.Sum256(p.Data[:])
	for i, s := range p.Siblings {
		if p.Leaf>>i&1 == 0 {
			h = hashPair(h, s)
		} else {
			h = hashPair(s, h)
		}
	}
	return h == root
}
func (db *DB) Verify() error {
	db.check()
	if len(db.undo) != 0 {
		return ErrPending
	}
	db.waitHash()
	root, err := db.rebuild()
	if err != nil {
		return err
	}
	if root != db.Root() {
		return fmt.Errorf("pagerdb: state differs from committed root")
	}
	return nil
}
