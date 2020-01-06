package filestore

import (
	"github.com/chuyangliu/rawkv/store"
)

type blockIndexEntry struct {
	key store.Key
	off store.KVLen
	len store.KVLen
}

func (bie *blockIndexEntry) size() store.KVLen {
	return store.KVLenSize + store.KVLen(len(bie.key)) + store.KVLenSize*2
}
