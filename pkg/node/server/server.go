package server

import (
	"context"

	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/rpc"
	"github.com/chuyangliu/rawkv/pkg/store"
	"github.com/chuyangliu/rawkv/pkg/store/shard"
)

var (
	logger = logging.New(logging.LevelInfo)
)

// Server manages the server running on node.
type Server struct {
	mgr *shard.Manager
}

// New instantiates a Server.
func New(rootdir string, flushThresh store.KVLen, blkSize store.KVLen) *Server {
	return &Server{
		mgr: shard.NewMgr(rootdir, flushThresh, blkSize),
	}
}

// --------------------------------
// rpc.StorageServer implementation
// --------------------------------

// Get returns the value associated with the key, and a boolean indicating whether the key exists.
func (s *Server) Get(ctx context.Context, req *rpc.GetReq) (*rpc.GetResp, error) {
	val, found, err := s.mgr.Get(store.Key(req.Key))
	resp := &rpc.GetResp{Val: []byte(val), Found: found}
	logger.Info("Get | req=%+v | resp=%+v", *req, *resp)
	return resp, err
}

// Put adds or updates a key-value pair to the storage.
func (s *Server) Put(ctx context.Context, req *rpc.PutReq) (*rpc.PutResp, error) {
	err := s.mgr.Put(store.Key(req.Key), store.Value(req.Val))
	resp := &rpc.PutResp{}
	logger.Info("Put | req=%+v | resp=%+v", *req, *resp)
	return resp, err
}

// Del removes key from the storage.
func (s *Server) Del(ctx context.Context, req *rpc.DelReq) (*rpc.DelResp, error) {
	err := s.mgr.Del(store.Key(req.Key))
	resp := &rpc.DelResp{}
	logger.Info("Del | req=%+v | resp=%+v", *req, *resp)
	return resp, err
}
