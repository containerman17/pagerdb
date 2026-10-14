package pagerdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"slices"
)

// pieceRun is the number of 40-byte undo references in one sorted run.
const pieceRun = 32768

// diskSort bounds sorting memory to 4096 records and two merge buffers.
// All scratch files belong to the caller's temporary directory.
func diskSort(dir string, src *os.File, size int, less func([]byte, []byte) bool) (io.ReadCloser, error) {
	if _, err := src.Seek(0, 0); err != nil {
		return nil, err
	}
	return diskSortRuns(dir, func(b []byte) (int, error) { return io.ReadFull(src, b) }, size, less, nil)
}

func diskSortPieces(dir string, src *os.File) (io.ReadCloser, error) {
	if _, err := src.Seek(0, 0); err != nil {
		return nil, err
	}
	return diskSortPiecesBuffers(dir, func(b []byte) (int, error) { return io.ReadFull(src, b) }, &sealBuffers{})
}

func diskSortPiecesBuffers(dir string, fill func([]byte) (int, error), buffers *sealBuffers) (io.ReadCloser, error) {
	return diskSortRuns(dir, fill, 40, func(a, b []byte) bool {
		if le.Uint64(a) != le.Uint64(b) {
			return le.Uint64(a) < le.Uint64(b)
		}
		return le.Uint64(a[8:]) > le.Uint64(b[8:])
	}, buffers)
}

// diskSortRuns sorts what fill supplies. fill behaves like io.ReadFull: a short
// count means the input is exhausted. Input that fits one run never touches disk;
// the returned reader then aliases the run buffer until the next sort with it.
func diskSortRuns(dir string, fill func([]byte) (int, error), size int, less func([]byte, []byte) bool, buffers *sealBuffers) (io.ReadCloser, error) {
	radix := buffers != nil
	runRecords := 4096
	if radix {
		runRecords = pieceRun
	}
	var run *os.File
	success := false
	defer func() {
		if !success && run != nil {
			run.Close()
		}
	}()
	var writer *bufio.Writer
	var buf, scratch []byte
	var rows [][]byte
	if radix {
		if buffers.sort == nil {
			buffers.sort = make([]byte, size*runRecords)
			buffers.radix = make([]byte, size*runRecords)
		}
		buf, scratch = buffers.sort, buffers.radix
	} else {
		buf, scratch = make([]byte, size*runRecords), make([]byte, size*runRecords)
		rows = make([][]byte, runRecords)
	}
	var total int64
	for {
		n, e := fill(buf)
		if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
			return nil, e
		}
		if n == 0 {
			break
		}
		if n%size != 0 {
			return nil, io.ErrUnexpectedEOF
		}
		sorted := buf[:n]
		if radix {
			// Twelve passes leave the result back in buf.
			radixPieces(buf[:n], scratch[:n])
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
			for i, r := range rows[:n/size] {
				copy(scratch[i*size:], r)
			}
			sorted = scratch[:n]
		}
		if run == nil && n < len(buf) {
			success = true
			return io.NopCloser(bytes.NewReader(sorted)), nil
		}
		if run == nil {
			var err error
			if run, err = os.CreateTemp(dir, "sort-"); err != nil {
				return nil, err
			}
			writer = bufio.NewWriterSize(run, 64<<10)
		}
		if _, e = writer.Write(sorted); e != nil {
			return nil, e
		}
		total += int64(n)
	}
	if run == nil {
		success = true
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if err := writer.Flush(); err != nil {
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
	_, err := run.Seek(0, 0)
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
