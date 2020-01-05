package memstore

import (
	"sync"

	"github.com/chuyangliu/rawkv/algods/treemap"
	"github.com/chuyangliu/rawkv/store"
)

// MemStore stores key-value data in memory.
type MemStore struct {
	data *treemap.TreeMap // map key to MemStoreEntry
	size store.KVLen      // number of bytes occupied by data
	lock sync.RWMutex
}

// New instantiates an empty MemStore.
func New() *MemStore {
	return &MemStore{
		data: treemap.New(store.KeyCmp),
		size: 0,
	}
}

// Entries returns all stored entries sorted by entry.key.
// Note that access to returned *store.Entry is unprotected.
func (ms *MemStore) Entries() []*store.Entry {
	raws := ms.data.Values()
	entries := make([]*store.Entry, len(raws))
	for i := 0; i < len(raws); i++ {
		entries[i], _ = raws[i].(*store.Entry)
	}
	return entries
}

// Size returns number of bytes needed to persist data on disk.
func (ms *MemStore) Size() store.KVLen {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	return ms.size
}

func (ms *MemStore) put(key store.Key, val store.Value) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	entry := &store.Entry{
		Key:  key,
		Val:  val,
		Stat: store.KStatPut,
	}
	ms.data.Put(key, entry)
	ms.size += entry.Size()
}

func (ms *MemStore) get(key store.Key) (store.Value, bool) {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	if entry, found := ms.getEntry(key); found && entry.Stat == store.KStatPut {
		return entry.Val, true
	}
	return "", false
}

func (ms *MemStore) del(key store.Key) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	if entry, found := ms.getEntry(key); found { // update only if entry exists
		ms.size -= entry.Size()
		entry.Val = ""
		entry.Stat = store.KStatDel
		ms.size += entry.Size()
	}
}

func (ms *MemStore) getEntry(key store.Key) (*store.Entry, bool) {
	if rawEntry, found := ms.data.Get(key); found {
		entry, _ := rawEntry.(*store.Entry)
		return entry, true
	}
	return nil, false
}
