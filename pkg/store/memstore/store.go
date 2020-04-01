// Package memstore implements the in-memory storage layer of an LSM tree.
package memstore

import (
	"sync"

	"github.com/chuyangliu/rawkv/pkg/algods/treemap"
	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/store"
)

// Store implements the in-memory storage layer of an LSM tree.
type Store struct {
	logger *logging.Logger
	lock   sync.RWMutex // Reader/writer lock to protect concurrent accesses.
	data   *treemap.Map // Tree map to store key-value pairs, mapping store.Key to store.Entry.
	size   store.KVLen  // Number of bytes required to persist data on disk.
}

// New creates a Store with given logging level.
func New(logLevel int) *Store {
	return &Store{
		logger: logging.New(logLevel),
		data:   treemap.New(store.KeyCmp),
		size:   0,
	}
}

// Entries returns all stored entries sorted ascendingly by key.
// Accesses to the returned slice are not thread-safe.
func (s *Store) Entries() []*store.Entry {
	raws := s.data.Values()
	entries := make([]*store.Entry, len(raws))
	for i := 0; i < len(raws); i++ {
		entries[i], _ = raws[i].(*store.Entry)
	}
	return entries
}

// Size returns number of bytes required to persist in-memory data on disk.
func (s *Store) Size() store.KVLen {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.size
}

// Get returns the entry associated with key, or nil if key is not found.
// If key is found, the returned entry is a copy of the original entry to prevent modifications to the original.
func (s *Store) Get(key store.Key) *store.Entry {
	s.lock.RLock()
	defer s.lock.RUnlock()
	entry := s.getUnsafe(key)
	if entry == nil {
		return nil
	}
	copy := *entry
	return &copy
}

// Put adds a key-value pair to the store.
// If key exists, the associated value is updated to the new value.
// Otherwise, a new key-value pair is added.
func (s *Store) Put(key store.Key, value store.Value) {
	s.lock.Lock()
	defer s.lock.Unlock()
	entry := &store.Entry{
		Key:    key,
		Value:  value,
		Status: store.StatusPut,
	}
	s.data.Put(key, entry)
	s.size += entry.Size()
}

// Del removes key from the store.
func (s *Store) Del(key store.Key) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if entry := s.getUnsafe(key); entry == nil {
		entry = &store.Entry{
			Key:    key,
			Value:  "",
			Status: store.StatusDel,
		}
		s.data.Put(key, entry)
		s.size += entry.Size()
	} else {
		s.size -= entry.Size()
		entry.Value = ""
		entry.Status = store.StatusDel
		s.size += entry.Size()
	}
}

// getUnsafe returns the entry associated with key, or nil if key is not found.
// Method is not thread-safe.
func (s *Store) getUnsafe(key store.Key) *store.Entry {
	if rawEntry, found := s.data.Get(key); found {
		entry, _ := rawEntry.(*store.Entry)
		return entry
	}
	return nil
}
