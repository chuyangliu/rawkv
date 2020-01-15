package server

import (
	"context"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/server/pb"
	"github.com/chuyangliu/rawkv/pkg/store"
	"github.com/chuyangliu/rawkv/pkg/store/shardmgr"
)

var (
	logger = logging.New(logging.LevelInfo)
)

// Server manages the server running on node.
type Server struct {
	rootdir string
	mgr     *shardmgr.Manager
}

// New instantiates a Server.
func New(rootdir string, flushThresh store.KVLen, blkSize store.KVLen) *Server {
	return &Server{
		rootdir: rootdir,
		mgr:     shardmgr.New(rootdir, flushThresh, blkSize),
	}
}

// Serve runs the server instance and start handling incoming requests.
func (s *Server) Serve(storageAddr string) error {

	// create root directory
	if err := os.MkdirAll(s.rootdir, 0777); err != nil {
		return fmt.Errorf("Create root directory failed | rootdir=%v | err=[%w]", s.rootdir, err)
	}

	// create listener for storage server
	listener, err := net.Listen("tcp", storageAddr)
	if err != nil {
		return fmt.Errorf("Storage server listen failed | addr=%v | err=[%w]", storageAddr, err)
	}

	// start gRPC server
	svr := grpc.NewServer()
	pb.RegisterStorageServer(svr, s)
	logger.Info("Storage server started | addr=%v | rootdir=%v", listener.Addr().String(), s.rootdir)
	svr.Serve(listener)

	return nil
}

// --------------------------------
// pb.StorageServer implementation
// --------------------------------

// Get returns the value associated with the key, and a boolean indicating whether the key exists.
func (s *Server) Get(ctx context.Context, req *pb.GetReq) (*pb.GetResp, error) {
	val, found, err := s.mgr.Get(store.Key(req.Key))
	resp := &pb.GetResp{Val: []byte(val), Found: found}
	return resp, err
}

// Put adds or updates a key-value pair to the storage.
func (s *Server) Put(ctx context.Context, req *pb.PutReq) (*pb.PutResp, error) {
	err := s.mgr.Put(store.Key(req.Key), store.Value(req.Val))
	resp := &pb.PutResp{}
	return resp, err
}

// Del removes key from the storage.
func (s *Server) Del(ctx context.Context, req *pb.DelReq) (*pb.DelResp, error) {
	err := s.mgr.Del(store.Key(req.Key))
	resp := &pb.DelResp{}
	return resp, err
}