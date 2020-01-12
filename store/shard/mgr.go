package shard

import (
	"github.com/chuyangliu/rawkv/store"
)

// Manager manages a collection of Shards.
// Currently only a single shard is managed as shard split hasn't been implemented.
type Manager struct {
	shards []*Shard
}

// NewMgr instantiates a ShardManager.
func NewMgr(rootdir string, flushThresh store.KVLen, blkSize store.KVLen) *Manager {
	return &Manager{
		shards: []*Shard{NewShard(rootdir, flushThresh, blkSize)},
	}
}

// Get returns the entry associated with the key, or nil if not exist.
func (m *Manager) Get(key store.Key) (*store.Entry, error) {
	return m.shards[0].Get(key)
}

// Put adds or updates a key-value pair to the shards.
func (m *Manager) Put(key store.Key, val store.Value) error {
	return m.shards[0].Put(key, val)
}

// Del removes key from the shards.
func (m *Manager) Del(key store.Key) error {
	return m.shards[0].Del(key)
}
