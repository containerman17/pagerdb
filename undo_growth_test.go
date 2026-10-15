package pagerdb

import "testing"

func TestUndoGrowth(t *testing.T) {
	db := openTest(t, Options{})
	r := allocTest(t, db, 2<<20)
	// A large setup block must not size every later undo buffer.
	db.Touch(r, 0, 2<<20)
	commitTest(t, db)
	must(t, db.Flush())
	const writes = 500
	var value [32]byte
	for block := 0; block < 2*len(db.blocks); block++ {
		grows := 0
		for i := 0; i < writes; i++ {
			before := cap(db.undo)
			db.Write(r, i*32, value[:])
			if cap(db.undo) != before {
				grows++
				if cap(db.undo) < 2*before {
					t.Fatal("undo growth did not double capacity")
				}
			}
		}
		if block > 0 && grows > 1 {
			t.Fatalf("block %d grew %d times after size was known", block, grows)
		}
		if block >= len(db.blocks) && grows != 0 {
			t.Fatalf("warm block %d grew %d times", block, grows)
		}
		commitTest(t, db)
		must(t, db.Flush())
		if db.undoHint != writes*(len(value)+16) {
			t.Fatal("large setup block propagated into undo hint")
		}
	}
	must(t, db.Verify())
}
