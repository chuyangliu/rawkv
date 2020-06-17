package server

import (
	"context"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/pb"
	"github.com/chuyangliu/rawkv/pkg/raft"
	"github.com/chuyangliu/rawkv/pkg/store"
	"github.com/chuyangliu/rawkv/pkg/store/shardmgr"
)

// Server manages the server running on node.
type Server struct {
	rootdir    string
	shardMgr   *shardmgr.Manager
	raftEngine *raft.Engine
	logger     *logging.Logger
}

// New instantiates a Server.
func New(rootdir string, flushThresh store.KVLen, blkSize store.KVLen, logLevel int) (*Server, error) {

	// create root directory
	if err := os.MkdirAll(rootdir, 0777); err != nil {
		return nil, fmt.Errorf("Create root directory failed | rootdir=%v | err=[%w]", rootdir, err)
	}

	// create Raft engine
	raftEngine, err := raft.NewEngine(rootdir, logLevel)
	if err != nil {
		return nil, fmt.Errorf("Create raft engine failed | rootdir=%v | err=[%w]", rootdir, err)
	}

	// instantiate
	return &Server{
		rootdir:    rootdir,
		shardMgr:   shardmgr.New(rootdir, flushThresh, blkSize, logLevel),
		raftEngine: raftEngine,
		logger:     logging.New(logLevel),
	}, nil
}

// Serve runs the server instance and start handling incoming requests.
func (s *Server) Serve(storageAddr string, raftAddr string) error {
	go s.serveStorage(storageAddr)
	go s.serveRaft(raftAddr)
	s.raftEngine.Run()
	return nil
}

func (s *Server) serveStorage(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		s.logger.Error("Storage server listen failed | addr=%v | err=[%v]", addr, err)
		return
	}
	svr := grpc.NewServer()
	pb.RegisterStorageServer(svr, s)
	s.logger.Info("Starting storage server | addr=%v | rootdir=%v", listener.Addr().String(), s.rootdir)
	if err := svr.Serve(listener); err != nil {
		s.logger.Error("Starting storage server failed | err=[%v]", err)
	}
}

func (s *Server) serveRaft(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		s.logger.Error("Raft server listen failed | addr=%v | err=[%v]", addr, err)
		return
	}
	svr := grpc.NewServer()
	pb.RegisterRaftServer(svr, s)
	s.logger.Info("Starting raft server | addr=%v | rootdir=%v", listener.Addr().String(), s.rootdir)
	if err := svr.Serve(listener); err != nil {
		s.logger.Error("Starting raft server failed | err=[%v]", err)
	}
}

// -------------------------------
// pb.StorageServer Implementation
// -------------------------------

// Get returns the value associated with the key, and a boolean indicating whether the key exists.
func (s *Server) Get(ctx context.Context, req *pb.GetReq) (*pb.GetResp, error) {
	s.logger.Debug("Get | key=%v", store.Key(req.Key))
	val, found, err := s.shardMgr.Get(store.Key(req.Key))
	resp := &pb.GetResp{Val: []byte(val), Found: found}
	return resp, err
}

// Put adds or updates a key-value pair to the storage.
func (s *Server) Put(ctx context.Context, req *pb.PutReq) (*pb.PutResp, error) {
	s.logger.Debug("Put | key=%v | val=%v", store.Key(req.Key), store.Value(req.Val))
	err := s.shardMgr.Put(store.Key(req.Key), store.Value(req.Val))
	resp := &pb.PutResp{}
	return resp, err
}

// Del removes key from the storage.
func (s *Server) Del(ctx context.Context, req *pb.DelReq) (*pb.DelResp, error) {
	s.logger.Debug("Del | key=%v", store.Key(req.Key))
	err := s.shardMgr.Del(store.Key(req.Key))
	resp := &pb.DelResp{}
	return resp, err
}

// ----------------------------
// pb.RaftServer Implementation
// ----------------------------

// RequestVote invoked by candidates to gather votes.
func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {
	return s.raftEngine.RequestVoteHandler(req)
}

// AppendEntries invoked by leader to replicate log entries, also used as heartbeat.
func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesReq) (*pb.AppendEntriesResp, error) {
	return s.raftEngine.AppendEntriesHandler(req)
}
