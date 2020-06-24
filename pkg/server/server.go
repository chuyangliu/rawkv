package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/chuyangliu/rawkv/pkg/cluster"
	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/pb"
	"github.com/chuyangliu/rawkv/pkg/raft"
	"github.com/chuyangliu/rawkv/pkg/store"
	"github.com/chuyangliu/rawkv/pkg/store/shardmgr"
)

// Server manages the server running on node.
type Server struct {
	logger   *logging.Logger
	initDone bool

	rootdir  string
	shardMgr *shardmgr.Manager

	clusterMeta cluster.Meta
	raftEngine  *raft.Engine
}

// New instantiates a Server.
func New(logLevel int, rootdir string, flushThresh store.KVLen, blkSize store.KVLen) (*Server, error) {

	// create root directory
	if err := os.MkdirAll(rootdir, 0777); err != nil {
		return nil, fmt.Errorf("Create root directory failed | rootdir=%v | err=[%w]", rootdir, err)
	}

	return &Server{
		logger:   logging.New(logLevel),
		initDone: false,
		rootdir:  rootdir,
		shardMgr: shardmgr.New(rootdir, flushThresh, blkSize, logLevel),
	}, nil
}

// Serve runs the server instance and start handling incoming requests.
func (s *Server) Serve(storageAddr string, raftAddr string) error {
	var err error

	// start grpc servers
	go s.serveStorage(storageAddr)
	go s.serveRaft(raftAddr)

	// create cluster meta
	s.clusterMeta, err = cluster.NewKubeMeta(s.logger.Level())
	if err != nil {
		return fmt.Errorf("Create cluster meta failed | err=[%w]", err)
	}

	// create raft engine
	s.raftEngine, err = raft.NewEngine(s.logger.Level(), s.rootdir, s.applyRaftLog, s.clusterMeta)
	if err != nil {
		return fmt.Errorf("Create raft engine failed | rootdir=%v | err=[%w]", s.rootdir, err)
	}

	// signal init complete
	s.initDone = true

	// start raft engine
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

func (s *Server) applyRaftLog(log *raft.Log) error {
	switch log.Cmd() {
	case raft.CmdPut:
		return s.shardMgr.Put(log.Key(), log.Val())
	case raft.CmdDel:
		return s.shardMgr.Del(log.Key())
	default:
		return fmt.Errorf("Unsupported log command | cmd=%v", log.Cmd())
	}
}

// -------------------------------
// pb.StorageServer Implementation
// -------------------------------

// Get returns the value associated with the key, and a boolean indicating whether the key exists.
func (s *Server) Get(ctx context.Context, req *pb.GetReq) (*pb.GetResp, error) {
	s.logger.Debug("Get | key=%v", store.Key(req.Key))
	if !s.initDone {
		return nil, errors.New("Server not ready")
	}
	val, found, err := s.shardMgr.Get(store.Key(req.Key))
	resp := &pb.GetResp{Val: []byte(val), Found: found}
	return resp, err
}

// Put adds or updates a key-value pair to the storage.
func (s *Server) Put(ctx context.Context, req *pb.PutReq) (*pb.PutResp, error) {
	s.logger.Debug("Put | key=%v | val=%v", store.Key(req.Key), store.Value(req.Val))
	if !s.initDone {
		return nil, errors.New("Server not ready")
	}
	err := s.raftEngine.Persist(raft.NewLog(raft.CmdPut, req.GetKey(), req.GetVal()))
	resp := &pb.PutResp{}
	return resp, err
}

// Del removes key from the storage.
func (s *Server) Del(ctx context.Context, req *pb.DelReq) (*pb.DelResp, error) {
	s.logger.Debug("Del | key=%v", store.Key(req.Key))
	if !s.initDone {
		return nil, errors.New("Server not ready")
	}
	err := s.raftEngine.Persist(raft.NewLog(raft.CmdDel, req.GetKey(), nil))
	resp := &pb.DelResp{}
	return resp, err
}

// ----------------------------
// pb.RaftServer Implementation
// ----------------------------

// RequestVote invoked by candidates to gather votes.
func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {
	if !s.initDone {
		return nil, errors.New("Server not ready")
	}
	return s.raftEngine.RequestVoteHandler(req)
}

// AppendEntries invoked by leader to replicate log entries, also used as heartbeat.
func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesReq) (*pb.AppendEntriesResp, error) {
	if !s.initDone {
		return nil, errors.New("Server not ready")
	}
	return s.raftEngine.AppendEntriesHandler(req)
}
