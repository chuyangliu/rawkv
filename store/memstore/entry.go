package memstore

import (
	"github.com/chuyangliu/rawkv/store"
)

// Entry stores each entry in MemStore.
type Entry struct {
	key  store.Key
	val  store.Value
	stat store.KStat
}

func (e *Entry) size() store.KVLen {
	return store.KVLen(store.KVLenSize + len(e.key) + store.KVLenSize + len(e.val) + store.KStatSize)
}
