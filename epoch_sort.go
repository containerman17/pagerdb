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
	buf := make([]byte, size*4096)
	rows := make([][]byte, 4096)
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
		total += int64(n)
	}
	if err = writer.Flush(); err != nil {
		return nil, err
	}
	x, y := make([]byte, size), make([]byte, size)
	a := bufio.NewReaderSize(bytes.NewReader(nil), 64<<10)
	b := bufio.NewReaderSize(bytes.NewReader(nil), 64<<10)
	var left, right io.SectionReader
	for width := int64(size * 4096); width < total; width *= 2 {
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
