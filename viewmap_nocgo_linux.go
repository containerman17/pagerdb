//go:build linux && !cgo

package pagerdb

func (vw *View) faultMap(r Region, l loc) (*viewMapping, error) {
	return nil, errFaultUnavailable
}
