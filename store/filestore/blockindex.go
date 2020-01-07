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

type blockIndex struct {
	data []*blockIndexEntry
}

func newBlockIndex() *blockIndex {
	return &blockIndex{
		data: make([]*blockIndexEntry, 0),
	}
}

func (bi *blockIndex) empty() bool {
	return len(bi.data) == 0
}

func (bi *blockIndex) last() *blockIndexEntry {
	return bi.data[len(bi.data)-1]
}

func (bi *blockIndex) add(entry *blockIndexEntry) {
	bi.data = append(bi.data, entry)
}

func (bi *blockIndex) entries() []*blockIndexEntry {
	return bi.data
}
