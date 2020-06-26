package shardmgr

import (
	"fmt"

	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/raft"
	"github.com/chuyangliu/rawkv/pkg/store"
	sd "github.com/chuyangliu/rawkv/pkg/store/shard"
)

// Manager manages a collection of Shards.
// Currently only a single shard is managed as shard split hasn't been implemented.
type Manager struct {
	logger     *logging.Logger
	shards     []*sd.Shard
	raftEngine *raft.Engine
}

// New instantiates a new Manager.
func New(logLevel int, rootdir string, flushThresh store.KVLen, blockSize store.KVLen,
	raftEngine *raft.Engine) *Manager {

	m := &Manager{
		logger:     logging.New(logLevel),
		shards:     []*sd.Shard{sd.New(logLevel, rootdir, flushThresh, blockSize)},
		raftEngine: raftEngine,
	}

	if raftEngine != nil {
		raftEngine.SetApplyFunc(m.applyRaftLog)
	}

	return m
}

// Get returns the value associated with the key, and a boolean indicating whether the key exists.
func (m *Manager) Get(key []byte) (store.Value, bool, error) {
	entry, err := m.shards[0].Get(store.Key(key))
	if err != nil {
		return "", false, fmt.Errorf("Get key from shard failed | err=[%w]", err)
	} else if entry == nil || entry.Stat == store.KStatDel {
		return "", false, nil
	}
	return entry.Val, true, nil
}

// Put adds or updates a key-value pair to the shards.
func (m *Manager) Put(key []byte, val []byte) error {
	var err error

	if m.raftEngine == nil {
		// no raft cluster, put key/val locally
		err = m.shards[0].Put(store.Key(key), store.Value(val))
	} else {
		err = m.raftEngine.Persist(raft.NewPutLog(key, val))
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
		// no raft cluster, delete key locally
		err = m.shards[0].Del(store.Key(key))
	} else {
		err = m.raftEngine.Persist(raft.NewDelLog(key))
	}

	if err != nil {
		return fmt.Errorf("Delete key from shard failed | err=[%w]", err)
	}

	return nil
}

func (m *Manager) applyRaftLog(log *raft.Log) error {
	switch log.Cmd() {
	case raft.CmdPut:
		return m.shards[0].Put(log.Key(), log.Val())
	case raft.CmdDel:
		return m.shards[0].Del(log.Key())
	default:
		return fmt.Errorf("Unsupported log command | cmd=%v", log.Cmd())
	}
}
