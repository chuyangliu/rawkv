package storage

import (
	"context"

	"github.com/chuyangliu/rawkv/logging"
	"github.com/chuyangliu/rawkv/store"
	"github.com/chuyangliu/rawkv/store/shard"
)

var (
	logger = logging.New(logging.LevelInfo)
)

// DefaultStorageServer is a default implementation of StorageServer interface.
type DefaultStorageServer struct {
	mgr *shard.Manager
}

// NewServer instantiates a storage server.
func NewServer(rootdir string, flushThresh store.KVLen, blkSize store.KVLen) *DefaultStorageServer {
	return &DefaultStorageServer{
		mgr: shard.NewMgr(rootdir, flushThresh, blkSize),
	}
}

// Get returns the value associated with the key, and a boolean indicating whether the key exists.
func (s *DefaultStorageServer) Get(ctx context.Context, req *GetReq) (*GetResp, error) {
	val, found, err := s.mgr.Get(store.Key(req.Key))
	resp := &GetResp{Val: []byte(val), Found: found}
	logger.Info("Get | req=%+v | resp=%+v", *req, *resp)
	return resp, err
}

// Put adds or updates a key-value pair to the storage.
func (s *DefaultStorageServer) Put(ctx context.Context, req *PutReq) (*PutResp, error) {
	err := s.mgr.Put(store.Key(req.Key), store.Value(req.Val))
	resp := &PutResp{}
	logger.Info("Put | req=%+v | resp=%+v", *req, *resp)
	return resp, err
}

// Del removes key from the storage.
func (s *DefaultStorageServer) Del(ctx context.Context, req *DelReq) (*DelResp, error) {
	err := s.mgr.Del(store.Key(req.Key))
	resp := &DelResp{}
	logger.Info("Del | req=%+v | resp=%+v", *req, *resp)
	return resp, err
}
