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
	return store.KVLenSize + store.KVLen(len(e.key)) + store.KVLenSize + store.KVLen(len(e.val)) + store.KStatSize
}
