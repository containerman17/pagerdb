//go:build linux && cgo

package pagerdb

/*
#cgo CFLAGS: -pthread
#cgo LDFLAGS: -pthread
#include "viewmap_native_linux.h"
*/
import "C"

import (
	"fmt"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// faultMap uses a native thread so a fault in unpreemptible Go code cannot
// deadlock garbage collection. The thread reads only immutable snapshot bytes.
func (vw *View) faultMap(r Region, l loc) (*viewMapping, error) {
	size := alignUp(l.size, uint64(unix.Getpagesize()))
	memory, err := unix.Mmap(-1, 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	var handler *C.struct_pagerdb_fault
	code := C.pagerdb_fault_open(unsafe.Pointer(&memory[0]), C.uint64_t(size), &handler)
	if code != 0 {
		unix.Munmap(memory)
		return nil, fmt.Errorf("%w: %s", errFaultUnavailable, syscall.Errno(code))
	}
	file, err := vw.snapshotView(l)
	if err != nil {
		C.pagerdb_fault_close(handler)
		unix.Munmap(memory)
		return nil, err
	}
	defer file.Close()
	var segments *C.uint64_t
	if len(l.segs) > 0 {
		segments = (*C.uint64_t)(unsafe.Pointer(&l.segs[0]))
	}
	code = C.pagerdb_fault_start(handler, C.int(file.Fd()), C.uint64_t(l.off), C.uint64_t(l.size), segments, C.size_t(len(l.segs)))
	runtime.KeepAlive(l.segs)
	if code != 0 {
		C.pagerdb_fault_close(handler)
		unix.Munmap(memory)
		return nil, fmt.Errorf("pagerdb: start native userfaultfd: %s", syscall.Errno(code))
	}
	return &viewMapping{vw: vw, loc: l, memory: memory, data: memory[:l.size:l.size], closeFault: func() { C.pagerdb_fault_close(handler) }}, nil
}
