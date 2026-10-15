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

// syncImage unmaps the image before syncing it. The data stays dirty in the page
// cache, so writeback has no mapped pages to write-protect; cleaning mapped pages
// cost the writer TLB-flush interrupts. Unmapped clean pages are reclaimed first.
func syncImage(mapped []byte, fd int) error {
	if err := unix.Madvise(mapped, unix.MADV_DONTNEED); err != nil {
		return err
	}
	return unix.Fdatasync(fd)
}
