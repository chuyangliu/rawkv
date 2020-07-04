// Package treemap implements a tree map to store key-value pairs in sorted key order.
package treemap

import (
	"fmt"
	"strings"

	gtm "github.com/emirpasic/gods/maps/treemap"
	gutils "github.com/emirpasic/gods/utils"
)

// Map implements a map (backed by a red-black tree) that stores key-value pairs in sorted key order.
// It supports arbitrary types as keys and values as long as a compare function for keys is provided.
// Methods of the map are not thread-safe.
type Map struct {
	tree *gtm.Map
}

// Cmp represents the compare function for two keys.
// Return -1 if k1 < k2, 1 if k1 > k2, 0 if k1 == k2.
type Cmp func(k1, k2 interface{}) int

// New creates a Map with cmp as the compare function.
func New(cmp Cmp) *Map {
	return &Map{tree: gtm.NewWith(gutils.Comparator(cmp))}
}

// String returns a string representation of the map.
func (m *Map) String() string {
	str := "treemap["
	it := m.tree.Iterator()
	for it.Next() {
		str += fmt.Sprintf("%v:%v ", it.Key(), it.Value())
	}
	return strings.TrimRight(str, " ") + "]"
}

// Size returns the number of key-value pairs in the map.
func (m *Map) Size() int {
	return m.tree.Size()
}

// Get returns the value associated with key, or nil if key is not found.
// The second return value found is set to true if key is found, and false otherwise.
func (m *Map) Get(key interface{}) (value interface{}, found bool) {
	return m.tree.Get(key)
}

// Put adds a key-value pair to the map.
// If key exists, the associated value is updated to the new value.
// Otherwise, a new key-value pair is added.
func (m *Map) Put(key interface{}, value interface{}) {
	m.tree.Put(key, value)
}

// Keys returns a copy of all keys in the map in ascending order.
func (m *Map) Keys() []interface{} {
	return m.tree.Keys()
}

// Values returns a copy of all values in the map in ascending key order.
func (m *Map) Values() []interface{} {
	return m.tree.Values()
}
