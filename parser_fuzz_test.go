package pagerdb

import (
	"hash/crc32"
	"testing"
)

func FuzzLogRecords(f *testing.F) {
	cp := make([]byte, cpRec)
	cp[0] = 'C'
	le.PutUint32(cp[48:], crc32.Checksum(cp[8:48], castagnoli))
	f.Add(cp)
	b := append([]byte(nil), cp...)
	e := make([]byte, entryHdr+2)
	e[0] = 'E'
	le.PutUint32(e[8:], 1)
	le.PutUint64(e[16:], 1)
	le.PutUint32(e[4:], entryChecksum(e, 1))
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
	le.PutUint32(cp[48:], crc32.Checksum(cp[8:48], castagnoli))
	block := make([]byte, blockRec)
	block[0] = 'B'
	le.PutUint64(block[8:], 1)
	le.PutUint32(block[48:], crc32.Checksum(block[4:48], castagnoli))
	f.Add(append(cp, block...))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<20 {
			return
		}
		scan(b)
	})
}
