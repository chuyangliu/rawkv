package filestore

import (
	"github.com/chuyangliu/rawkv/pkg/store"
)

// blockIndexEntry represents an entry stored in a block index.
type blockIndexEntry struct {
	key store.Key
	off store.KVLen
	len store.KVLen
}

// size returns the number of bytes required to persist the block index entry on disk.
func (bie *blockIndexEntry) size() store.KVLen {
	return store.KVLenSize + store.KVLen(len(bie.key)) + store.KVLenSize*2
}

// blockIndex stores the index to the blocks of store file.
type blockIndex struct {
	entries []*blockIndexEntry
}

// newBlockIndex creates a blockIndex.
func newBlockIndex() *blockIndex {
	return &blockIndex{entries: make([]*blockIndexEntry, 0)}
}

// empty returns whether the block index has entries.
func (bi *blockIndex) empty() bool {
	return len(bi.entries) == 0
}

// last returns the last entry of the block index.
func (bi *blockIndex) last() *blockIndexEntry {
	return bi.entries[len(bi.entries)-1]
}

// add adds an entry to the block index.
func (bi *blockIndex) add(entry *blockIndexEntry) {
	bi.entries = append(bi.entries, entry)
}

// get searches the block index and returns the entry corresponding to given key.
// Return nil if no such entry can be found.
func (bi *blockIndex) get(key store.Key) *blockIndexEntry {
	n := len(bi.entries)
	if n == 0 || key < bi.entries[0].key {
		return nil // Key not found.
	}
	lo, hi := 0, n
	for lo < hi { // Search upperbound.
		mid := lo + (hi-lo)/2
		if bi.entries[mid].key <= key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return bi.entries[lo-1]
}

// fileBlock represents a block of store file.
type fileBlock struct {
	entries []*store.Entry
}

// newBlock creates a fileBlock.
func newBlock() *fileBlock {
	return &fileBlock{entries: make([]*store.Entry, 0)}
}

// add adds an entry to the file block.
func (fb *fileBlock) add(entry *store.Entry) {
	fb.entries = append(fb.entries, entry)
}

// get searches the file block and returns the entry corresponding to given key.
// Return nil if no such entry can be found.
func (fb *fileBlock) get(key store.Key) *store.Entry {
	n := len(fb.entries)
	lo, hi := 0, n-1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		if fb.entries[mid].Key < key {
			lo = mid + 1
		} else if fb.entries[mid].Key > key {
			hi = mid - 1
		} else {
			return fb.entries[mid]
		}
	}
	return nil // Key not found.
}
