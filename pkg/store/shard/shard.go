// Package shard provides methods to manage a range of key-value data.
package shard

import (
	"fmt"
	"path"
	"sync"

	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/store"
	"github.com/chuyangliu/rawkv/pkg/store/filestore"
	"github.com/chuyangliu/rawkv/pkg/store/memstore"
)

// Shard manages a range of key-value data.
type Shard struct {
	logger      *logging.Logger
	lock        sync.RWMutex       // Reader/writer lock to protect concurrent accesses.
	rootdir     string             // Path to the root directory for store files.
	flushThresh store.KVLen        // MemStore's flush threshold in bytes.
	blockSize   store.KVLen        // Block size of store file in bytes.
	mem         *memstore.Store    // MemStore to serve all writes and part of reads.
	files       []*filestore.Store // FileStores from oldest to newest to serve part of reads.
}

// New creates a Shard with given logging level, root directory, flush threshold, and block size.
func New(level int, rootdir string, flushThresh store.KVLen, blockSize store.KVLen) *Shard {
	return &Shard{
		logger:      logging.New(level),
		rootdir:     rootdir,
		flushThresh: flushThresh,
		blockSize:   blockSize,
		mem:         memstore.New(level),
		files:       make([]*filestore.Store, 0),
	}
}

// Get returns the entry associated with key, or nil if key is not found.
func (s *Shard) Get(key store.Key) (*store.Entry, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// Search MemStore.
	if entry := s.mem.Get(key); entry != nil {
		return entry, nil
	}

	// Search FileStores from newest to oldest.
	for i := len(s.files) - 1; i >= 0; i-- {
		if entry, err := s.files[i].Get(key); err != nil {
			return nil, fmt.Errorf("Read FileStore failed | key=%v | index=%v | rootdir=%v | err=[%w]",
				key, i, s.rootdir, err)
		} else if entry != nil {
			return entry, nil
		}
	}

	return nil, nil
}

// Put adds a key-value pair to the shard.
// If key exists, the associated value is updated to the new value.
// Otherwise, a new key-value pair is added.
func (s *Shard) Put(key store.Key, value store.Value) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Write MemStore.
	s.mem.Put(key, value)

	// Flush if size above threshold.
	if s.mem.Size() >= s.flushThresh {
		if err := s.flush(); err != nil {
			return fmt.Errorf("Flush failed | key=%v | value=%v | rootdir=%v | err=[%w]", key, value, s.rootdir, err)
		}
	}

	return nil
}

// Del removes key from the shard.
func (s *Shard) Del(key store.Key) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Write MemStore.
	s.mem.Del(key)

	// Flush if size above threshold.
	if s.mem.Size() >= s.flushThresh {
		if err := s.flush(); err != nil {
			return fmt.Errorf("Flush failed | key=%v | rootdir=%v | err=[%w]", key, s.rootdir, err)
		}
	}

	return nil
}

// flush flushes the MemStore to a store file and creates a new empty MemStore.
func (s *Shard) flush() error {
	filePath := s.nextFilePath()

	fs, err := filestore.New(s.logger.Level(), filePath, s.mem)
	if err != nil {
		return fmt.Errorf("Create FileStore failed | path=%v | err=[%w]", filePath, err)
	}

	fs.BeginFlush(s.blockSize)
	s.files = append(s.files, fs)
	s.mem = memstore.New(s.logger.Level())

	return nil
}

// nextFilePath returns the next available path to a new store file.
func (s *Shard) nextFilePath() string {
	return path.Join(s.rootdir, fmt.Sprintf("%v.filestore", len(s.files)))
}
