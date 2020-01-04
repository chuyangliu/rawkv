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
// Note that access to returned *Entry is unprotected.
func (ms *MemStore) Entries() []*Entry {
	raws := ms.data.Values()
	entries := make([]*Entry, len(raws))
	for i := 0; i < len(raws); i++ {
		entries[i], _ = raws[i].(*Entry)
	}
	return entries
}

func (ms *MemStore) getSize() store.KVLen {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	return ms.size
}

func (ms *MemStore) put(key store.Key, val store.Value) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	entry := &Entry{
		key:  key,
		val:  val,
		stat: store.KStatPut,
	}
	ms.data.Put(key, entry)
	ms.size += entry.size()
}

func (ms *MemStore) get(key store.Key) (store.Value, bool) {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	if entry, found := ms.getEntry(key); found && entry.stat == store.KStatPut {
		return entry.val, true
	}
	return "", false
}

func (ms *MemStore) del(key store.Key) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	if entry, found := ms.getEntry(key); found { // update only if entry exists
		ms.size -= entry.size()
		entry.val = ""
		entry.stat = store.KStatDel
		ms.size += entry.size()
	}
}

func (ms *MemStore) getEntry(key store.Key) (*Entry, bool) {
	if rawEntry, found := ms.data.Get(key); found {
		entry, _ := rawEntry.(*Entry)
		return entry, true
	}
	return nil, false
}
