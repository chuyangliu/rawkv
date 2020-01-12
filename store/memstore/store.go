package memstore

import (
	"sync"

	"github.com/chuyangliu/rawkv/algods/treemap"
	"github.com/chuyangliu/rawkv/store"
)

// Store stores key-value data in memory.
type Store struct {
	data *treemap.Map // map key to store.Entry
	size store.KVLen  // number of bytes occupied by data
	lock sync.RWMutex
}

// New instantiates an empty MemStore.
func New() *Store {
	return &Store{
		data: treemap.New(store.KeyCmp),
		size: 0,
	}
}

// Entries returns all stored entries sorted by entry.key.
// Note that access to returned *store.Entry is unprotected.
func (s *Store) Entries() []*store.Entry {
	raws := s.data.Values()
	entries := make([]*store.Entry, len(raws))
	for i := 0; i < len(raws); i++ {
		entries[i], _ = raws[i].(*store.Entry)
	}
	return entries
}

// Size returns number of bytes needed to persist data on disk.
func (s *Store) Size() store.KVLen {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.size
}

// Put adds a key-value pair to the store.
func (s *Store) Put(key store.Key, val store.Value) {
	s.lock.Lock()
	defer s.lock.Unlock()
	entry := &store.Entry{
		Key:  key,
		Val:  val,
		Stat: store.KStatPut,
	}
	s.data.Put(key, entry)
	s.size += entry.Size()
}

// Get returns the value associated with the key, and whether the key exists.
func (s *Store) Get(key store.Key) *store.Entry {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.getUnsafe(key)
}

// Del removes key from the store.
func (s *Store) Del(key store.Key) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if entry := s.getUnsafe(key); entry == nil {
		entry = &store.Entry{
			Key:  key,
			Val:  "",
			Stat: store.KStatDel,
		}
		s.data.Put(key, entry)
		s.size += entry.Size()
	} else {
		s.size -= entry.Size()
		entry.Val = ""
		entry.Stat = store.KStatDel
		s.size += entry.Size()
	}
}

func (s *Store) getUnsafe(key store.Key) *store.Entry {
	if rawEntry, found := s.data.Get(key); found {
		entry, _ := rawEntry.(*store.Entry)
		return entry
	}
	return nil // key not exist
}
