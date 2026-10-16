package pagerdb

import (
	"slices"
	"testing"
)

func TestDirtyBitmapOrder(t *testing.T) {
	db := openTest(t, Options{MaxLeaves: testLeaves})
	for _, want := range [][]uint32{
		{0, 1, 63, 64, 65, 127, 128, 4095},
		{},
		{64},
		{0, 4095},
	} {
		// Reverse order, repeated marks and ranges crossing word boundaries.
		for i := len(want) - 1; i >= 0; i-- {
			for range 2 {
				db.markDirty(uint64(want[i])*LeafSize, LeafSize)
			}
		}
		if len(want) == 8 {
			db.markDirty(63*LeafSize+1, LeafSize)
			db.markDirty(127*LeafSize, 2*LeafSize)
		}
		db.markDirty(20*LeafSize, 0)
		slot := &db.snapshots[db.nextSnapshot]
		commitTest(t, db)
		must(t, db.Flush())
		if !slices.Equal(slot.ids, want) {
			t.Fatalf("snapshot IDs = %v, want %v", slot.ids, want)
		}
		for _, word := range db.dirtyBits {
			if word != 0 {
				t.Fatal("dirty word survived snapshot")
			}
		}
		if db.dirtyStart != uint64(len(db.dirtyBits)) || db.dirtyEnd != 0 {
			t.Fatal("dirty bounds survived snapshot")
		}
	}
}
