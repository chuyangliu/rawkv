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

// Shard stores a range of key-value data.
type Shard struct {
	logger      *logging.Logger
	rootdir     string             // path to root directory to persist FileStores
	flushThresh store.KVLen        // threshold in bytes to flush MemStore
	blockSize   store.KVLen        // block size in bytes of FileStore
	mem         *memstore.Store    // single MemStore
	files       []*filestore.Store // multiple FileStores from oldest to newest
	lock        sync.RWMutex
}

// New instantiates an empty Shard.
func New(logLevel int, rootdir string, flushThresh store.KVLen, blockSize store.KVLen) *Shard {
	return &Shard{
		logger:      logging.New(logLevel),
		rootdir:     rootdir,
		flushThresh: flushThresh,
		blockSize:   blockSize,
		mem:         memstore.New(logLevel),
		files:       make([]*filestore.Store, 0),
	}
}

// Get returns the entry associated with the key, or nil if not exist.
func (s *Shard) Get(key store.Key) (*store.Entry, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	// search MemStore
	if entry := s.mem.Get(key); entry != nil {
		return entry, nil
	}
	// search FileStores from newest to oldest
	for i := len(s.files) - 1; i >= 0; i-- {
		if entry, err := s.files[i].Get(key); err != nil {
			return nil, fmt.Errorf("Read FileStore failed | rootdir=%v | index=%v | err=[%w]", s.rootdir, i, err)
		} else if entry != nil {
			return entry, nil
		}
	}
	return nil, nil
}

// Put adds or updates a key-value pair to the shard.
func (s *Shard) Put(key store.Key, val store.Value) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	// write MemStore
	s.mem.Put(key, val)
	// check flush
	if s.mem.Size() >= s.flushThresh {
		if err := s.flush(); err != nil {
			return fmt.Errorf("Flush failed | rootdir=%v | err=[%w]", s.rootdir, err)
		}
	}
	return nil
}

// Del removes key from the store.
func (s *Shard) Del(key store.Key) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	// write MemStore
	s.mem.Del(key)
	// check flush
	if s.mem.Size() >= s.flushThresh {
		if err := s.flush(); err != nil {
			return fmt.Errorf("Flush failed | rootdir=%v | err=[%w]", s.rootdir, err)
		}
	}
	return nil
}

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

func (s *Shard) nextFilePath() string {
	return path.Join(s.rootdir, fmt.Sprintf("%v.filestore", len(s.files)))
}
