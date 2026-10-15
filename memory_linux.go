package pagerdb

import "golang.org/x/sys/unix"

func anonymous(n int) ([]byte, error) {
	b, err := unix.Mmap(-1, 0, n, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err == nil {
		_ = unix.Madvise(b, unix.MADV_HUGEPAGE)
	}
	return b, err
}
func dataSync(fd int) error { return unix.Fdatasync(fd) }
