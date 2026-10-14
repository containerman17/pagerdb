package pagerdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"slices"
)

// diskSort bounds sorting memory to 4096 records and two merge buffers.
// All scratch files belong to the caller's temporary directory.
func diskSort(dir string, src *os.File, size int, less func([]byte, []byte) bool) (*os.File, error) {
	return diskSortRuns(dir, src, size, less, nil)
}

func diskSortPieces(dir string, src *os.File) (*os.File, error) {
	return diskSortPiecesBuffers(dir, src, &sealBuffers{})
}

func diskSortPiecesBuffers(dir string, src *os.File, buffers *sealBuffers) (*os.File, error) {
	return diskSortRuns(dir, src, 40, func(a, b []byte) bool {
		if le.Uint64(a) != le.Uint64(b) {
			return le.Uint64(a) < le.Uint64(b)
		}
		return le.Uint64(a[8:]) > le.Uint64(b[8:])
	}, buffers)
}

func diskSortRuns(dir string, src *os.File, size int, less func([]byte, []byte) bool, buffers *sealBuffers) (*os.File, error) {
	radix := buffers != nil
	runRecords := 4096
	if radix {
		runRecords = 32768
	}
	if _, err := src.Seek(0, 0); err != nil {
		return nil, err
	}
	run, err := os.CreateTemp(dir, "sort-")
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			run.Close()
		}
	}()
	writer := bufio.NewWriterSize(run, 64<<10)
	var buf, radixScratch []byte
	var rows [][]byte
	if radix {
		if buffers.sort == nil {
			buffers.sort = make([]byte, size*runRecords)
			buffers.radix = make([]byte, size*runRecords)
		}
		buf, radixScratch = buffers.sort, buffers.radix
	} else {
		buf = make([]byte, size*runRecords)
		rows = make([][]byte, runRecords)
	}
	var total int64
	for {
		n, e := io.ReadFull(src, buf)
		if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
			return nil, e
		}
		if n == 0 {
			break
		}
		if n%size != 0 {
			return nil, io.ErrUnexpectedEOF
		}
		if radix {
			radixPieces(buf[:n], radixScratch[:n])
			if _, e = writer.Write(buf[:n]); e != nil {
				return nil, e
			}
		} else {
			for i := 0; i < n/size; i++ {
				rows[i] = buf[i*size : (i+1)*size]
			}
			slices.SortStableFunc(rows[:n/size], func(a, b []byte) int {
				if less(a, b) {
					return -1
				}
				if less(b, a) {
					return 1
				}
				return 0
			})
			for _, r := range rows[:n/size] {
				if _, e = writer.Write(r); e != nil {
					return nil, e
				}
			}
		}
		total += int64(n)
	}
	if err = writer.Flush(); err != nil {
		return nil, err
	}
	x, y := make([]byte, size), make([]byte, size)
	a := bufio.NewReaderSize(bytes.NewReader(nil), 64<<10)
	b := bufio.NewReaderSize(bytes.NewReader(nil), 64<<10)
	var left, right io.SectionReader
	for width := int64(size * runRecords); width < total; width *= 2 {
		next, e := os.CreateTemp(dir, "merge-")
		if e != nil {
			return nil, e
		}
		writer.Reset(next)
		for start := int64(0); start < total; start += 2 * width {
			mid, end := min(start+width, total), min(start+2*width, total)
			left = *io.NewSectionReader(run, start, mid-start)
			a.Reset(&left)
			right = *io.NewSectionReader(run, mid, end-mid)
			b.Reset(&right)
			_, ex := io.ReadFull(a, x)
			_, ey := io.ReadFull(b, y)
			for ex == nil || ey == nil {
				takeX := ey != nil || ex == nil && !less(y, x)
				if takeX {
					_, e = writer.Write(x)
					_, ex = io.ReadFull(a, x)
				} else {
					_, e = writer.Write(y)
					_, ey = io.ReadFull(b, y)
				}
				if e != nil {
					next.Close()
					return nil, e
				}
			}
			if ex != io.EOF || ey != io.EOF {
				next.Close()
				return nil, io.ErrUnexpectedEOF
			}
		}
		if e = writer.Flush(); e != nil {
			next.Close()
			return nil, e
		}
		run.Close()
		os.Remove(run.Name())
		run = next
	}
	_, err = run.Seek(0, 0)
	success = err == nil
	return run, err
}

func pieceLess(a, b []byte) bool {
	x, y := binary.LittleEndian.Uint64(a), binary.LittleEndian.Uint64(b)
	if x != y {
		return x < y
	}
	return binary.LittleEndian.Uint64(a[8:]) < binary.LittleEndian.Uint64(b[8:])
}
func rootLess(a, b []byte) bool { return bytes.Compare(a[:32], b[:32]) < 0 }

// radixPieces sorts fixed undo references by leaf ascending, then log position
// descending. Twelve stable byte passes use two fixed record buffers.
func radixPieces(records, scratch []byte) {
	for pass := 0; pass < 12; pass++ {
		keyAt := pass + 8
		if pass >= 8 {
			keyAt = pass - 8
		}
		var counts [256]int
		for at := 0; at < len(records); at += 40 {
			key := records[at+keyAt]
			if pass < 8 {
				key = ^key
			}
			counts[key]++
		}
		next := 0
		for i, n := range counts {
			counts[i], next = next, next+n*40
		}
		for at := 0; at < len(records); at += 40 {
			key := records[at+keyAt]
			if pass < 8 {
				key = ^key
			}
			dst := counts[key]
			copy(scratch[dst:dst+40], records[at:at+40])
			counts[key] += 40
		}
		records, scratch = scratch, records
	}
}
