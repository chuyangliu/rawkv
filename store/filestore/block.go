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
	return &blockIndex{data: make([]*blockIndexEntry, 0)}
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

func (bi *blockIndex) get(key store.Key) *blockIndexEntry {
	n := len(bi.data)
	if n == 0 || key < bi.data[0].key {
		return nil // no blocks contain key
	}
	lo, hi := 0, n
	for lo < hi { // search upperbound
		mid := lo + (hi-lo)/2
		if bi.data[mid].key <= key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return bi.data[lo-1]
}

type fileBlock struct {
	data []*store.Entry
}

func newBlock() *fileBlock {
	return &fileBlock{data: make([]*store.Entry, 0)}
}

func (fb *fileBlock) add(entry *store.Entry) {
	fb.data = append(fb.data, entry)
}

func (fb *fileBlock) get(key store.Key) *store.Entry {
	n := len(fb.data)
	lo, hi := 0, n-1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		if fb.data[mid].Key < key {
			lo = mid + 1
		} else if fb.data[mid].Key > key {
			hi = mid - 1
		} else {
			return fb.data[mid]
		}
	}
	return nil // key not exist
}
