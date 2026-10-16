//go:build !pagerdb_leaf512 && !pagerdb_leaf1k

package pagerdb

// LeafSize is a consensus constant: it changes every root. Build with the tag
// pagerdb_leaf512 or pagerdb_leaf1k to measure a smaller leaf.
const LeafSize = 4096
