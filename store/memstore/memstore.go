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

// Put adds a key-value pair to the store.
func (ms *MemStore) Put(key store.Key, val store.Value) {
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

// Get returns the value associated with the key, and whether the key exists.
func (ms *MemStore) Get(key store.Key) *store.Entry {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	return ms.getUnsafe(key)
}

// Del removes key from the store.
func (ms *MemStore) Del(key store.Key) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	if entry := ms.getUnsafe(key); entry == nil {
		entry = &store.Entry{
			Key:  key,
			Val:  "",
			Stat: store.KStatDel,
		}
		ms.data.Put(key, entry)
		ms.size += entry.Size()
	} else {
		ms.size -= entry.Size()
		entry.Val = ""
		entry.Stat = store.KStatDel
		ms.size += entry.Size()
	}
}

func (ms *MemStore) getUnsafe(key store.Key) *store.Entry {
	if rawEntry, found := ms.data.Get(key); found {
		entry, _ := rawEntry.(*store.Entry)
		return entry
	}
	return nil // key not exist
}
