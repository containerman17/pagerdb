//go:build !pagerdb_leaf512 && !pagerdb_leaf4k

package pagerdb

// LeafSize is a consensus constant: it changes every root.
// The pagerdb_leaf1k tag remains an alias for this default.
const LeafSize = 1024
