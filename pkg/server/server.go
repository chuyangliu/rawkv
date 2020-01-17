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

// Server manages the server running on node.
type Server struct {
	rootdir string
	mgr     *shardmgr.Manager
	logger  *logging.Logger
}

// New instantiates a Server.
func New(rootdir string, flushThresh store.KVLen, blkSize store.KVLen, level int) *Server {
	return &Server{
		rootdir: rootdir,
		mgr:     shardmgr.New(rootdir, flushThresh, blkSize),
		logger:  logging.New(level),
	}
}

// Serve runs the server instance and start handling incoming requests.
func (s *Server) Serve(storageAddr string, raftAddr string) error {

	// create root directory
	if err := os.MkdirAll(s.rootdir, 0777); err != nil {
		return fmt.Errorf("Create root directory failed | rootdir=%v | err=[%w]", s.rootdir, err)
	}

	// create listener for storage server
	storageListener, err := net.Listen("tcp", storageAddr)
	if err != nil {
		return fmt.Errorf("Storage server listen failed | addr=%v | err=[%w]", storageAddr, err)
	}

	// start storage server
	storageSvr := grpc.NewServer()
	pb.RegisterStorageServer(storageSvr, s)
	go func() {
		s.logger.Info("Starting storage server | addr=%v | rootdir=%v", storageListener.Addr().String(), s.rootdir)
		if err := storageSvr.Serve(storageListener); err != nil {
			s.logger.Error("Starting storage server failed | err=%v", err)
		}
	}()

	// create listener for raft server
	raftListener, err := net.Listen("tcp", raftAddr)
	if err != nil {
		return fmt.Errorf("Raft server listen failed | addr=%v | err=[%w]", storageAddr, err)
	}

	// start raft server
	raftSvr := grpc.NewServer()
	pb.RegisterRaftServer(raftSvr, s)
	s.logger.Info("Starting raft server | addr=%v | rootdir=%v", raftListener.Addr().String(), s.rootdir)
	if err := raftSvr.Serve(raftListener); err != nil {
		s.logger.Error("Starting raft server failed | err=%v", err)
	}

	return nil
}

// --------------------------------
// pb.StorageServer implementation
// --------------------------------

// Get returns the value associated with the key, and a boolean indicating whether the key exists.
func (s *Server) Get(ctx context.Context, req *pb.GetReq) (*pb.GetResp, error) {
	s.logger.Debug("Get | key=%v", store.Key(req.Key))
	val, found, err := s.mgr.Get(store.Key(req.Key))
	resp := &pb.GetResp{Val: []byte(val), Found: found}
	return resp, err
}

// Put adds or updates a key-value pair to the storage.
func (s *Server) Put(ctx context.Context, req *pb.PutReq) (*pb.PutResp, error) {
	s.logger.Debug("Put | key=%v | val=%v", store.Key(req.Key), store.Value(req.Val))
	err := s.mgr.Put(store.Key(req.Key), store.Value(req.Val))
	resp := &pb.PutResp{}
	return resp, err
}

// Del removes key from the storage.
func (s *Server) Del(ctx context.Context, req *pb.DelReq) (*pb.DelResp, error) {
	s.logger.Debug("Del | key=%v", store.Key(req.Key))
	err := s.mgr.Del(store.Key(req.Key))
	resp := &pb.DelResp{}
	return resp, err
}

// --------------------------------
// pb.RaftServer implementation
// --------------------------------

// RequestVote invoked by candidates to gather votes
func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {
	s.logger.Debug("RequestVote")
	resp := &pb.RequestVoteResp{}
	return resp, nil
}

// AppendEntries invoked by leader to replicate log entries, also used as heartbeat.
func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesReq) (*pb.AppendEntriesResp, error) {
	s.logger.Debug("AppendEntries")
	resp := &pb.AppendEntriesResp{}
	return resp, nil
}
