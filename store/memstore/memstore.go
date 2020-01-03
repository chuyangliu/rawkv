package memstore

import (
	"sync"

	"github.com/chuyangliu/rawkv/algods/treemap"
	"github.com/chuyangliu/rawkv/store"
)

// MemStore stores key-value data in memory
type MemStore struct {
	data *treemap.TreeMap // map key to memStoreVal
	size store.KVLen      // number of bytes occupied by data
	lock sync.RWMutex
}

type memStoreVal struct {
	val  store.Value
	stat store.KStat
}

// NewStore instantiates an empty MemStore
func NewStore() *MemStore {
	return &MemStore{
		data: treemap.NewMap(store.KeyCmp),
		size: 0,
	}
}

func (ms *MemStore) getSize() store.KVLen {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	return ms.size
}

func (ms *MemStore) put(key store.Key, val store.Value) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	ms.data.Put(key, &memStoreVal{
		val:  val,
		stat: store.KStatPut,
	})
	ms.size += entrySize(key, val)
}

func (ms *MemStore) get(key store.Key) (store.Value, bool) {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	return ms.getUnsafe(key)
}

func (ms *MemStore) del(key store.Key) {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	if val, found := ms.getUnsafe(key); found { // update only if key exists
		ms.size -= entrySize(key, val)
		ms.data.Put(key, &memStoreVal{
			val:  "",
			stat: store.KStatDel,
		})
		ms.size += entrySize(key, "")
	}
}

func (ms *MemStore) getUnsafe(key store.Key) (store.Value, bool) {
	if rawVal, found := ms.data.Get(key); found {
		msVal, _ := rawVal.(*memStoreVal)
		if msVal.stat == store.KStatPut {
			return msVal.val, true
		}
	}
	return "", false
}

func entrySize(key store.Key, val store.Value) store.KVLen {
	return store.KVLen(store.KVLenSize + len(key) + store.KVLenSize + len(val) + store.KStatSize)
}
