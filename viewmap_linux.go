package pagerdb

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"sync/atomic"
	"unsafe"
)

const (
	uffdioAPI      = 0xc018aa3f
	uffdioRegister = 0xc020aa00
	uffdioCopy     = 0xc028aa03
)

func uffdIoctl(fd int, op uintptr, arg unsafe.Pointer) error {
	_, _, e := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), op, uintptr(arg))
	if e != 0 {
		return e
	}
	return nil
}
func (vw *View) faultMap(r Region, l loc) (*viewMapping, error) {
	fd0, _, errno := unix.Syscall(unix.SYS_USERFAULTFD, uintptr(unix.O_CLOEXEC|unix.O_NONBLOCK|1), 0, 0)
	if errno != 0 {
		return nil, fmt.Errorf("%w: %s", errFaultUnavailable, errno)
	}
	fd := int(fd0)
	api := [3]uint64{0xaa, 0, 0}
	if err := uffdIoctl(fd, uffdioAPI, unsafe.Pointer(&api[0])); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("%w: %s", errFaultUnavailable, err)
	}
	size := alignUp(l.size, uint64(unix.Getpagesize()))
	b, err := unix.Mmap(-1, 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		unix.Close(fd)
		return nil, err
	}
	addr := uint64(uintptr(unsafe.Pointer(&b[0])))
	reg := [4]uint64{addr, size, 1, 0}
	if err = uffdIoctl(fd, uffdioRegister, unsafe.Pointer(&reg[0])); err != nil {
		unix.Munmap(b)
		unix.Close(fd)
		return nil, fmt.Errorf("%w: %s", errFaultUnavailable, err)
	}
	var stop atomic.Bool
	done := make(chan struct{})
	m := &viewMapping{vw: vw, loc: l, memory: b, data: b[:l.size:l.size]}
	m.closeFault = func() { stop.Store(true); <-done; unix.Close(fd) }
	go func() {
		defer close(done)
		page := make([]byte, unix.Getpagesize())
		var msg [32]byte
		for !stop.Load() {
			poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
			n, err := unix.Poll(poll, 50)
			if err == unix.EINTR {
				continue
			}
			if err != nil {
				panic(fmt.Sprintf("pagerdb: userfaultfd poll: %v", err))
			}
			if n == 0 {
				continue
			}
			if poll[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
				panic("pagerdb: userfaultfd poll descriptor failed")
			}
			n, err = unix.Read(fd, msg[:])
			if err == unix.EAGAIN || err == unix.EINTR {
				continue
			}
			if err != nil || n != 32 || msg[0] != 0x12 {
				panic(fmt.Sprintf("pagerdb: userfaultfd invalid event: bytes=%d event=%d error=%v", n, msg[0], err))
			}
			fault := le.Uint64(msg[16:]) / uint64(len(page)) * uint64(len(page))
			off := fault - addr
			if fault < addr || off >= l.size {
				panic("pagerdb: userfaultfd address outside mapping")
			}
			clear(page)
			count := min(uint64(len(page)), l.size-off)
			// Failure must never fabricate snapshot bytes. Fail the process rather than
			// resolve a page with unverified content after storage corruption.
			if err := vw.Read(r, int(off), int(count), page); err != nil {
				panic(fmt.Sprintf("pagerdb: userfaultfd snapshot read: %v", err))
			}
			cp := [5]uint64{fault, uint64(uintptr(unsafe.Pointer(&page[0]))), uint64(len(page)), 0, 0}
			if err := uffdIoctl(fd, uffdioCopy, unsafe.Pointer(&cp[0])); err != nil && err != unix.EEXIST {
				panic(fmt.Sprintf("pagerdb: userfaultfd copy: %v", err))
			}
		}
	}()
	return m, nil
}

func snapshotState(db *DB) (*os.File, error) {
	f, err := os.CreateTemp(db.dir, ".view-")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	if err = unix.IoctlFileClone(int(f.Fd()), int(db.stateF.Fd())); err != nil {
		err = sparseSnapshot(db.stateF, f)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
