package pagerdb

import (
	"hash/crc32"
	"testing"
)

func FuzzLogRecords(f *testing.F) {
	cp := make([]byte, cpRec)
	cp[0] = 'C'
	f.Add(cp)
	b := append([]byte(nil), cp...)
	e := make([]byte, entryHdr+2)
	e[0] = 'E'
	le.PutUint32(e[8:], 1)
	le.PutUint64(e[16:], 1)
	b = append(b, e...)
	f.Add(b)
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			return
		}
		scan(b)
	})
}
func FuzzBlockRecords(f *testing.F) {
	cp := make([]byte, cpRec)
	cp[0] = 'C'
	block := make([]byte, blockRec)
	block[0] = 'B'
	le.PutUint64(block[8:], 1)
	le.PutUint32(block[48:], crc32.Checksum(nil, castagnoli))
	f.Add(append(cp, block...))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			return
		}
		scan(b)
	})
}
