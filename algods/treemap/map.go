package treemap

import (
	"fmt"
	"strings"

	gtm "github.com/emirpasic/gods/maps/treemap"
	gutils "github.com/emirpasic/gods/utils"
)

// Map maintains map keys in sorted order using a balanced tree.
type Map struct {
	tree *gtm.Map
}

// Cmp stores comparator function for keys, returns -1 if k1 < k2, 1 if k1 > k2, 0 if k1 == k2.
type Cmp func(k1, k2 interface{}) int

// New instantiates a tree map with a comparator for keys.
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

// Size returns number of elements in the map.
func (m *Map) Size() int {
	return m.tree.Size()
}

// Get returns the value associated with key, or nil if key doesn't exist.
func (m *Map) Get(key interface{}) (val interface{}, found bool) {
	return m.tree.Get(key)
}

// Put adds or updates a key-value pair to the map.
func (m *Map) Put(key interface{}, val interface{}) {
	m.tree.Put(key, val)
}

// Keys returns all keys in the map in ascending order.
func (m *Map) Keys() []interface{} {
	return m.tree.Keys()
}

// Values returns all values in the map sorted ascendingly by key.
func (m *Map) Values() []interface{} {
	return m.tree.Values()
}
