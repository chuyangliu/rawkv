// Package shardmgr provides methods to manage a group of shards.
package shardmgr

import (
	"fmt"

	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/raft"
	"github.com/chuyangliu/rawkv/pkg/store"
	sd "github.com/chuyangliu/rawkv/pkg/store/shard"
)

// Manager manages a group of shards.
// Currently only a single shard is managed as shard split hasn't been implemented.
type Manager struct {
	logger     *logging.Logger
	shards     []*sd.Shard
	raftEngine *raft.Engine
}

// New creates a Manager with given logging level, root directory, flush threshold, block size, and raft engine.
// If raftEngine is nil, perform operations locally (for testing).
func New(level int, rootdir string, flushThresh store.KVLen, blockSize store.KVLen, raftEngine *raft.Engine) *Manager {

	m := &Manager{
		logger:     logging.New(level),
		shards:     []*sd.Shard{sd.New(level, rootdir, flushThresh, blockSize)},
		raftEngine: raftEngine,
	}

	if raftEngine != nil {
		raftEngine.SetApplyFunc(m.applyRaftLog)
	}

	return m
}

// Get returns the value associated with key, or empty string if key is not found.
// The second return value is set to true if key is found, and false otherwise.
func (m *Manager) Get(key []byte) (store.Value, bool, error) {
	entry, err := m.shards[0].Get(store.Key(key))
	if err != nil {
		return "", false, fmt.Errorf("Get key from shard failed | err=[%w]", err)
	} else if entry == nil || entry.Status == store.StatusDel {
		return "", false, nil
	}
	return entry.Value, true, nil
}

// Put adds a key-value pair to the shards.
// If key exists, the associated value is updated to the new value.
// Otherwise, a new key-value pair is added.
func (m *Manager) Put(key []byte, value []byte) error {
	var err error

	if m.raftEngine == nil {
		// No raft cluster, put key-value locally.
		err = m.shards[0].Put(store.Key(key), store.Value(value))
	} else {
		err = m.raftEngine.Persist(raft.NewPutLog(key, value))
	}

	if err != nil {
		return fmt.Errorf("Put key to shard failed | err=[%w]", err)
	}

	return nil
}

// Del removes key from the shards.
func (m *Manager) Del(key []byte) error {
	var err error

	if m.raftEngine == nil {
		// No raft cluster, delete key locally.
		err = m.shards[0].Del(store.Key(key))
	} else {
		err = m.raftEngine.Persist(raft.NewDelLog(key))
	}

	if err != nil {
		return fmt.Errorf("Delete key from shard failed | err=[%w]", err)
	}

	return nil
}

// applyRaftLog applies given raft log to the shards.
func (m *Manager) applyRaftLog(log *raft.Log) error {
	switch log.Cmd() {
	case raft.CmdNoOp:
		return nil
	case raft.CmdPut:
		return m.shards[0].Put(log.Key(), log.Val())
	case raft.CmdDel:
		return m.shards[0].Del(log.Key())
	default:
		return fmt.Errorf("Unsupported log command | cmd=%v", log.Cmd())
	}
}
