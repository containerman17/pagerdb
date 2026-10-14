package pagerdb

import "testing"

func TestParseRewindIntent(t *testing.T) {
	valid := make([]byte, 32)
	copy(valid, "PGREWND2")
	le.PutUint64(valid[8:], 7)
	le.PutUint64(valid[16:], 2)
	le.PutUint64(valid[24:], cpRec)
	for _, tc := range []struct {
		name   string
		change func([]byte) []byte
		valid  bool
	}{
		{"valid", func(b []byte) []byte { return b }, true},
		{"short", func(b []byte) []byte { return b[:31] }, false},
		{"long", func(b []byte) []byte { return append(b, 0) }, false},
		{"magic", func(b []byte) []byte { b[0] = 'x'; return b }, false},
		{"epoch", func(b []byte) []byte { clear(b[16:24]); return b }, false},
		{"position", func(b []byte) []byte { clear(b[24:]); return b }, false},
		{"overflow", func(b []byte) []byte { le.PutUint64(b[24:], 1<<63); return b }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, e, p, err := parseRewindIntent(tc.change(append([]byte(nil), valid...)))
			if (err == nil) != tc.valid {
				t.Fatal(err)
			}
			if tc.valid && (v != 7 || e != 2 || p != cpRec) {
				t.Fatalf("%d %d %d", v, e, p)
			}
		})
	}
}

func FuzzRewindIntent(f *testing.F) {
	valid := make([]byte, 32)
	copy(valid, "PGREWND2")
	le.PutUint64(valid[8:], 7)
	le.PutUint64(valid[16:], 2)
	le.PutUint64(valid[24:], cpRec)
	f.Add(valid)
	f.Add([]byte("PGREWND2"))
	f.Fuzz(func(t *testing.T, b []byte) {
		v, e, p, err := parseRewindIntent(b)
		if err == nil && (len(b) != 32 || e == 0 || p < cpRec || p > 1<<63-1) {
			t.Fatalf("accepted %d %d %d", v, e, p)
		}
	})
}
