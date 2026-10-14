package pagerdb

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type sealBuffers struct {
	sort, radix, payload, compressed []byte
	encoder                          *zstd.Encoder
}

type sealSection struct {
	file                                 *os.File
	dirBase, denseBase, indexOff, leaves uint64
}

func (db *DB) sealSection(dir string, c *chunk, end uint64, sorted *os.File, previous *sealedEpoch, oldStart, high uint64, buffers *sealBuffers) (section sealSection, result error) {
	create := func(name string) (*os.File, error) { return os.CreateTemp(dir, name) }
	sr := bufio.NewReaderSize(sorted, 256<<10)
	result = func() error {
		out, err := create("epoch-")
		if err != nil {
			return err
		}
		success := false
		defer func() {
			if !success {
				out.Close()
			}
		}()
		if _, err = out.Write(make([]byte, epochHdr)); err != nil {
			return err
		}
		index, err := create("index-")
		if err != nil {
			return err
		}
		defer index.Close()
		iw := bufio.NewWriterSize(index, 256<<10)
		dense, err := create("dense-")
		if err != nil {
			return err
		}
		defer dense.Close()
		dw := bufio.NewWriterSize(dense, 64<<10)
		dirs, err := create("directories-")
		if err != nil {
			return err
		}
		defer dirs.Close()
		fw := bufio.NewWriterSize(dirs, 64<<10)
		if buffers.encoder == nil {
			buffers.encoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
			if err != nil {
				return err
			}
		}
		enc := buffers.encoder
		oldAt, nLeaves, dirBytes, denseBytes := oldStart, uint64(0), uint64(0), uint64(0)
		var copiedIndex [idxRec]byte
		copyOld := func(until uint64) error {
			if previous == nil {
				return nil
			}
			for oldAt < previous.nLeaves {
				var e error
				copiedIndex, e = previous.indexRecord(oldAt)
				if e != nil {
					return e
				}
				if uint64(le.Uint32(copiedIndex[:])) >= min(until, high) {
					break
				}
				clear(copiedIndex[4:16])
				clear(copiedIndex[24:48])
				if _, e = iw.Write(copiedIndex[:]); e != nil {
					return e
				}
				oldAt++
				nLeaves++
			}
			return nil
		}
		frameWriter := bufio.NewWriterSize(out, 1<<20)
		frameOffset := uint64(epochHdr)
		var frameHeader [frameHdr]byte
		if buffers.payload == nil {
			buffers.payload = make([]byte, 0, frameCap+LeafSize)
		}
		if buffers.compressed == nil {
			buffers.compressed = make([]byte, 0, frameCap+LeafSize)
		}
		payload, compressed := buffers.payload[:0], buffers.compressed[:0]
		defer func() { buffers.payload, buffers.compressed = payload[:0], compressed[:0] }()
		var rec [40]byte
		var inherited, indexRow [idxRec]byte
		var frameDirectory [frameDirRec]byte
		var denseRow [denseRec]byte
		_, readErr := io.ReadFull(sr, rec[:])
		for readErr == nil {
			leaf := le.Uint64(rec[:])
			if err = copyOld(leaf); err != nil {
				return err
			}
			prior := uint64(0)
			if previous != nil && oldAt < previous.nLeaves {
				r, e := previous.indexRecord(oldAt)
				if e != nil {
					return e
				}
				if uint64(le.Uint32(r[:])) == leaf {
					prior = le.Uint64(r[16:])
					inherited = r
					oldAt++
				}
			}
			leafDir, leafDense := dirBytes, denseBytes
			frames, count, firstIndex := uint32(0), uint64(0), uint64(0)
			runCount, newest, oldest := uint32(0), uint32(0), uint32(0)
			var boundary, delta [LeafSize]byte
			// Touch captures its final new bytes at Commit and can overlap later
			// writes. Reconstruct the epoch-end leaf once, then derive each write's
			// actual undo delta from the next newer state instead of stale new data.
			db.mu.Lock()
			db.readWindowRange(end, leaf, 0, boundary[:])
			db.mu.Unlock()
			payload = payload[:0]
			flush := func(full bool) error {
				if runCount == 0 {
					return nil
				}
				flags := byte(0)
				if full {
					payload = append(payload, boundary[:]...)
					flags |= 2
				}
				rawLen := len(payload)
				compressed = enc.EncodeAll(payload, compressed[:0])
				body := payload
				if len(compressed)*10 <= len(payload)*9 {
					body = compressed
					flags |= 1
				}
				off := frameOffset
				var e error
				h := frameHeader[:]
				h[0] = flags
				le.PutUint32(h[4:], uint32(len(body)))
				le.PutUint32(h[8:], uint32(rawLen))
				le.PutUint32(h[12:], runCount)
				le.PutUint64(h[16:], firstIndex)
				le.PutUint32(h[24:], oldest)
				le.PutUint32(h[28:], newest)
				if _, e = frameWriter.Write(h[:]); e != nil {
					return e
				}
				if _, e = frameWriter.Write(body); e != nil {
					return e
				}
				frameOffset += uint64(frameHdr + len(body))
				le.PutUint64(frameDirectory[:], off)
				le.PutUint64(frameDirectory[8:], firstIndex)
				le.PutUint32(frameDirectory[16:], runCount)
				le.PutUint32(frameDirectory[20:], uint32(flags))
				if _, e = fw.Write(frameDirectory[:]); e != nil {
					return e
				}
				dirBytes += frameDirRec
				frames++
				firstIndex = count
				runCount = 0
				payload = payload[:0]
				return nil
			}
			for readErr == nil && le.Uint64(rec[:]) == leaf {
				ver := uint32(le.Uint64(rec[16:]) - c.start)
				oldAt := le.Uint64(rec[24:])
				off, n := uint64(le.Uint16(rec[32:])), uint64(le.Uint16(rec[34:]))
				for i := uint64(0); i < n; i++ {
					delta[i] = boundary[off+i] ^ c.m[oldAt+i]
				}
				lo, hi := uint64(0), n
				for lo < hi && delta[lo] == 0 {
					lo++
				}
				for hi > lo && delta[hi-1] == 0 {
					hi--
				}
				if lo < hi {
					if len(payload)+denseRec+int(hi-lo) > frameCap {
						if err = flush(true); err != nil {
							return err
						}
					}
					le.PutUint32(denseRow[:], ver)
					le.PutUint16(denseRow[4:], uint16(off+lo))
					le.PutUint16(denseRow[6:], uint16(hi-lo))
					if _, err = dw.Write(denseRow[:]); err != nil {
						return err
					}
					denseBytes += denseRec
					if runCount == 0 {
						newest = ver
					}
					oldest = ver
					payload = append(payload, denseRow[:]...)
					payload = append(payload, delta[lo:hi]...)
					count++
					runCount++
				}
				copy(boundary[off:off+n], c.m[oldAt:oldAt+n])
				_, readErr = io.ReadFull(sr, rec[:])
			}
			if count == 0 {
				if prior != 0 {
					clear(inherited[4:16])
					clear(inherited[24:48])
					if _, err = iw.Write(inherited[:]); err != nil {
						return err
					}
					nLeaves++
				}
				continue
			}
			// A base copy at the epoch start bounds how much history a deep read
			// undoes: once the undo work since the last base would reach one
			// frame, this epoch's final run ends in a copy.
			base, cost := uint64(0), uint32(0)
			if prior != 0 {
				base, cost = le.Uint64(inherited[48:]), le.Uint32(inherited[56:])
			}
			cost += baseHopCost + uint32(len(payload))
			if frames > 0 {
				cost += frameCap
			}
			full := len(payload) >= frameCap || cost >= frameCap
			if err = flush(full); err != nil {
				return err
			}
			if full {
				base, cost = c.epoch, 0
			}
			le.PutUint32(indexRow[:], uint32(leaf))
			le.PutUint32(indexRow[4:], frames)
			le.PutUint64(indexRow[8:], leafDir)
			le.PutUint64(indexRow[16:], c.epoch)
			le.PutUint64(indexRow[24:], prior)
			le.PutUint64(indexRow[32:], leafDense)
			le.PutUint64(indexRow[40:], count)
			le.PutUint64(indexRow[48:], base)
			le.PutUint32(indexRow[56:], cost)
			if _, err = iw.Write(indexRow[:]); err != nil {
				return err
			}
			nLeaves++
		}
		if readErr != io.EOF {
			return readErr
		}
		if err = copyOld(^uint64(0)); err != nil {
			return err
		}
		for _, w := range []*bufio.Writer{iw, dw, fw} {
			if err = w.Flush(); err != nil {
				return err
			}
		}
		if err = frameWriter.Flush(); err != nil {
			return err
		}
		dirBase, err := out.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		dirs.Seek(0, 0)
		if _, err = io.Copy(out, dirs); err != nil {
			return err
		}
		denseBase, err := out.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		dense.Seek(0, 0)
		if _, err = io.Copy(out, dense); err != nil {
			return err
		}
		indexOff, err := out.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		index.Seek(0, 0)
		ir := bufio.NewReaderSize(index, 64<<10)
		ow := bufio.NewWriterSize(out, 64<<10)
		var r [idxRec]byte
		for i := uint64(0); i < nLeaves; i++ {
			if _, err = io.ReadFull(ir, r[:]); err != nil {
				return err
			}
			if le.Uint32(r[4:]) != 0 {
				le.PutUint64(r[8:], le.Uint64(r[8:])+uint64(dirBase))
				le.PutUint64(r[32:], le.Uint64(r[32:])+uint64(denseBase))
			}
			if _, err = ow.Write(r[:]); err != nil {
				return err
			}
		}
		if err = ow.Flush(); err != nil {
			return err
		}
		success = true
		section = sealSection{file: out, dirBase: uint64(dirBase), denseBase: uint64(denseBase), indexOff: uint64(indexOff), leaves: nLeaves}
		return nil

	}()
	return section, result
}

// parallelSeal assigns disjoint contiguous leaf ranges to workers. Each worker
// uses fixed sorting and compression buffers; only section descriptors scale
// with the worker count. Section offsets are relocated during sequential output.
func (db *DB) parallelSeal(dir string, c *chunk, end, nRoots, minLeaf, maxLeaf uint64, pieces, roots *os.File) (*os.File, error) {
	info, err := pieces.Stat()
	if err != nil {
		return nil, err
	}
	workers := min(runtime.GOMAXPROCS(0), max(1, int(info.Size()/40/4096)))
	var rec [40]byte
	reader := bufio.NewReaderSize(pieces, 256<<10)
	if minLeaf == ^uint64(0) {
		minLeaf = 0
	}
	// A fixed histogram balances clustered leaves without keeping one entry per leaf.
	const buckets = 4096
	width := (maxLeaf-minLeaf)/buckets + 1
	var counts [buckets]uint64
	if _, err = pieces.Seek(0, 0); err != nil {
		return nil, err
	}
	reader.Reset(pieces)
	for {
		_, err = io.ReadFull(reader, rec[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		counts[(le.Uint64(rec[:])-minLeaf)/width]++
	}
	bounds := make([]uint64, 0, workers+1)
	bounds = append(bounds, 0)
	var cumulative uint64
	for i, n := range counts {
		cumulative += n
		if n != 0 && len(bounds) < workers && cumulative >= uint64(info.Size()/40)*uint64(len(bounds))/uint64(workers) {
			boundary := minLeaf + uint64(i+1)*width
			if boundary <= maxLeaf {
				bounds = append(bounds, boundary)
			}
		}
	}
	bounds = append(bounds, uint64(1)<<32)
	workers = len(bounds) - 1
	var bucketWorker [buckets]int
	worker := 0
	for bucket := range bucketWorker {
		leaf := minLeaf + uint64(bucket)*width
		for worker+1 < workers && leaf >= bounds[worker+1] {
			worker++
		}
		bucketWorker[bucket] = worker
	}
	refs := make([]*os.File, workers)
	writers := make([]*bufio.Writer, workers)
	sections := make([]sealSection, workers)
	errs := make([]error, workers)
	defer func() {
		for _, f := range refs {
			if f != nil {
				f.Close()
			}
		}
		for _, s := range sections {
			if s.file != nil {
				s.file.Close()
			}
		}
	}()
	for i := range refs {
		refs[i], err = os.CreateTemp(dir, "range-")
		if err != nil {
			return nil, err
		}
		writers[i] = bufio.NewWriterSize(refs[i], 256<<10)
	}
	if _, err = pieces.Seek(0, 0); err != nil {
		return nil, err
	}
	reader.Reset(pieces)
	for {
		_, err = io.ReadFull(reader, rec[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		i := bucketWorker[(le.Uint64(rec[:])-minLeaf)/width]
		if _, err = writers[i].Write(rec[:]); err != nil {
			return nil, err
		}
	}
	for _, w := range writers {
		if err = w.Flush(); err != nil {
			return nil, err
		}
	}
	db.mu.Lock()
	var previous *sealedEpoch
	if len(db.sealed) > 0 {
		previous = db.sealed[len(db.sealed)-1]
	}
	db.mu.Unlock()
	if previous != nil && previous.end != c.start {
		return nil, fmt.Errorf("pagerdb: epoch checkpoint discontinuity")
	}
	for len(db.sealWorkers) < workers {
		db.sealWorkers = append(db.sealWorkers, sealBuffers{})
	}
	var wg sync.WaitGroup
	for i := range refs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sorted, e := diskSortPiecesBuffers(dir, refs[i], &db.sealWorkers[i])
			if e != nil {
				errs[i] = e
				return
			}
			defer sorted.Close()
			low, high := bounds[i], bounds[i+1]
			oldStart := uint64(0)
			if previous != nil {
				hi := previous.nLeaves
				for oldStart < hi {
					m := (oldStart + hi) / 2
					r, e := previous.indexRecord(m)
					if e != nil {
						errs[i] = e
						return
					}
					if uint64(le.Uint32(r[:])) < low {
						oldStart = m + 1
					} else {
						hi = m
					}
				}
			}
			sections[i], errs[i] = db.sealSection(dir, c, end, sorted, previous, oldStart, high, &db.sealWorkers[i])
		}(i)
	}
	sortedRoots, rootErr := diskSort(dir, roots, rootRec, rootLess)
	wg.Wait()
	if sortedRoots != nil {
		defer sortedRoots.Close()
	}
	if rootErr != nil {
		return nil, rootErr
	}
	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	out, err := os.CreateTemp(dir, "epoch-")
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			out.Close()
		}
	}()
	ow := bufio.NewWriterSize(out, 1<<20)
	var header [epochHdr]byte
	if _, err = ow.Write(header[:]); err != nil {
		return nil, err
	}
	offsets := make([]uint64, workers)
	position, nLeaves := uint64(epochHdr), uint64(0)
	var directory [frameDirRec]byte
	var sectionReader io.SectionReader
	copyRange := func(f *os.File, at, n uint64) error {
		sectionReader = *io.NewSectionReader(f, int64(at), int64(n))
		_, e := io.Copy(ow, &sectionReader)
		return e
	}
	for i, s := range sections {
		offsets[i] = position - epochHdr
		if err = copyRange(s.file, epochHdr, s.dirBase-epochHdr); err != nil {
			return nil, err
		}
		sectionReader = *io.NewSectionReader(s.file, int64(s.dirBase), int64(s.denseBase-s.dirBase))
		reader.Reset(&sectionReader)
		for at := s.dirBase; at < s.denseBase; at += frameDirRec {
			if _, err = io.ReadFull(reader, directory[:]); err != nil {
				return nil, err
			}
			le.PutUint64(directory[:], le.Uint64(directory[:])+offsets[i])
			if _, err = ow.Write(directory[:]); err != nil {
				return nil, err
			}
		}
		if err = copyRange(s.file, s.denseBase, s.indexOff-s.denseBase); err != nil {
			return nil, err
		}
		position += s.indexOff - epochHdr
		nLeaves += s.leaves
	}
	indexOff := position
	var row [idxRec]byte
	for i, s := range sections {
		sectionReader = *io.NewSectionReader(s.file, int64(s.indexOff), int64(s.leaves*idxRec))
		reader.Reset(&sectionReader)
		for j := uint64(0); j < s.leaves; j++ {
			if _, err = io.ReadFull(reader, row[:]); err != nil {
				return nil, err
			}
			if le.Uint32(row[4:]) != 0 {
				le.PutUint64(row[8:], le.Uint64(row[8:])+offsets[i])
				le.PutUint64(row[32:], le.Uint64(row[32:])+offsets[i])
			}
			if _, err = ow.Write(row[:]); err != nil {
				return nil, err
			}
		}
	}
	rootsOff := indexOff + nLeaves*idxRec
	if _, err = io.Copy(ow, sortedRoots); err != nil {
		return nil, err
	}
	if err = ow.Flush(); err != nil {
		return nil, err
	}
	copy(header[:], epochMagic)
	for i, v := range []uint64{c.epoch, c.start, end, nLeaves, indexOff, nRoots, rootsOff} {
		le.PutUint64(header[8+i*8:], v)
	}
	if _, err = out.WriteAt(header[:], 0); err != nil {
		return nil, err
	}
	if _, err = out.Seek(0, 0); err != nil {
		return nil, err
	}
	success = true
	return out, nil
}
