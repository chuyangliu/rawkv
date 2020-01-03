package treemap

import (
	"fmt"
	"strings"

	godsTreeMap "github.com/emirpasic/gods/maps/treemap"
	godsUtils "github.com/emirpasic/gods/utils"
)

// TreeMap maintains map keys in sorted order using a balanced tree
type TreeMap struct {
	tree *godsTreeMap.Map
}

// Cmp stores comparator function for keys, returns -1 if k1 < k2, 1 if k1 > k2, 0 if k1 == k2
type Cmp func(k1, k2 interface{}) int

// NewMap instantiates a tree map with a comparator for keys
func NewMap(cmp Cmp) *TreeMap {
	return &TreeMap{tree: godsTreeMap.NewWith(godsUtils.Comparator(cmp))}
}

// String returns a string representation of the map
func (tm *TreeMap) String() string {
	str := "treemap["
	it := tm.tree.Iterator()
	for it.Next() {
		str += fmt.Sprintf("%v:%v ", it.Key(), it.Value())
	}
	return strings.TrimRight(str, " ") + "]"
}

// Size returns number of elements in the map
func (tm *TreeMap) Size() int {
	return tm.tree.Size()
}

// Get returns the value associated with key, or nil if key doesn't exist
func (tm *TreeMap) Get(key interface{}) (val interface{}, found bool) {
	return tm.tree.Get(key)
}

// Put adds or updates a key-value pair to the map
func (tm *TreeMap) Put(key interface{}, val interface{}) {
	tm.tree.Put(key, val)
}

// Keys returns all keys in the map in ascending order
func (tm *TreeMap) Keys() []interface{} {
	return tm.tree.Keys()
}

// Values returns all values in the map sorted ascendingly by key
func (tm *TreeMap) Values() []interface{} {
	return tm.tree.Values()
}
