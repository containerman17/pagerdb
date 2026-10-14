package pagerdb

import (
	"crypto/sha256"
	"errors"
	"fmt"
)

// Proof is a leaf of the head state and the sibling hashes from the leaf up to the root.
type Proof struct {
	// Leaf is the state file offset divided by LeafSize.
	Leaf uint64
	// Data contains the proved leaf bytes.
	Data [LeafSize]byte
	// Siblings contains sibling hashes in leaf-to-root order.
	Siblings [][32]byte
}

// Proof returns the proof for the leaf holding byte off of region r at the committed head.
func (db *DB) Proof(r Region, off int) (Proof, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.dirty) > 0 {
		if db.seq() != db.blockSeq || len(db.touches) != 0 {
			return Proof{}, ErrPending
		}
		if db.hashDirty() != db.root {
			return Proof{}, fmt.Errorf("pagerdb: proof state differs from committed root")
		}
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

// Verify rehashes the allocated state, compares every tree node on the way
// with the stored tree and checks the result against the committed root. It
// reads the whole allocated state while holding the database lock. Open runs
// the same full check only after an unclean shutdown.
func (db *DB) Verify() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.seq() != db.blockSeq || len(db.touches) != 0 {
		return ErrPending
	}
	if len(db.dirty) > 0 {
		db.hashDirty() // undone writes can leave dirty leaves at committed bytes
	}
	fi, err := db.stateF.Stat()
	if err != nil {
		return err
	}
	span := min(10, db.depth)
	leaves := uint64(fi.Size()) / LeafSize
	chunk := uint64(1) << span
	roots := make([][32]byte, (leaves+chunk-1)/chunk)
	errs := make([]error, len(roots))
	parallel(len(roots), func(lo, hi int) {
		level := make([][32]byte, chunk)
		for c := lo; c < hi; c++ {
			first := uint64(c) * chunk
			for i := range level {
				if leaf := first + uint64(i); leaf < leaves {
					level[i] = sha256.Sum256(db.state[leaf*LeafSize : (leaf+1)*LeafSize])
				} else {
					level[i] = db.empty[0]
				}
			}
			errs[c] = db.checkLevel(level, db.maxLeaves+first, 0)
			for l := 1; l <= span && errs[c] == nil; l++ {
				n := chunk >> l
				for i := range n {
					level[i] = hashPair(level[2*i][:], level[2*i+1][:])
				}
				errs[c] = db.checkLevel(level[:n], (db.maxLeaves+first)>>l, l)
			}
			roots[c] = level[0]
		}
	})
	if err := errors.Join(errs...); err != nil {
		return err
	}
	// Above the chunks only a prefix of each level is nonempty.
	level := roots
	for l := span + 1; l <= db.depth; l++ {
		if len(level)%2 == 1 {
			level = append(level, db.empty[l-1])
		}
		for i := range len(level) / 2 {
			level[i] = hashPair(level[2*i][:], level[2*i+1][:])
		}
		level = level[:len(level)/2]
		if err := db.checkLevel(level, db.maxLeaves>>l, l); err != nil {
			return err
		}
	}
	if level[0] != db.root {
		return fmt.Errorf("pagerdb: verify: state root %x does not match committed root %x", level[0], db.root)
	}
	return nil
}

// checkLevel compares computed hashes of nodes first, first+1, ... at level
// with the stored tree.
func (db *DB) checkLevel(hashes [][32]byte, first uint64, level int) error {
	for i, h := range hashes {
		if [32]byte(db.node(first+uint64(i), level)) != h {
			return fmt.Errorf("pagerdb: verify: tree node %d at level %d differs from state", first+uint64(i), level)
		}
	}
	return nil
}
