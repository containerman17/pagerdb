package pagerdb

import "golang.org/x/sys/unix"

func anonymous(n int) ([]byte, error) {
	return unix.Mmap(-1, 0, n, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
}
func dataSync(fd int) error { return unix.Fsync(fd) }

func syncImage(mapped []byte, fd int) error { return unix.Msync(mapped, unix.MS_SYNC) }
