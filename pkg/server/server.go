package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"

	"github.com/chuyangliu/rawkv/pkg/cluster"
	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/pb"
	"github.com/chuyangliu/rawkv/pkg/raft"
	"github.com/chuyangliu/rawkv/pkg/store"
	"github.com/chuyangliu/rawkv/pkg/store/shardmgr"
)

// Assert *Server implements pb.StorageServer.
var _ pb.StorageServer = (*Server)(nil)

// Assert *Server implements pb.RaftServer.
var _ pb.RaftServer = (*Server)(nil)

// Server manages the server running on node.
type Server struct {
	logger      *logging.Logger
	rootdir     string
	flushThresh store.KVLen
	blockSize   store.KVLen

	initDone     bool
	initDoneLock sync.Mutex

	clusterMeta cluster.Meta
	raftEngine  *raft.Engine
	shardMgr    *shardmgr.Manager
}

// New instantiates a Server.
func New(logLevel int, rootdir string, flushThresh store.KVLen, blockSize store.KVLen) (*Server, error) {

	// create root directory
	if err := os.MkdirAll(rootdir, 0777); err != nil {
		return nil, fmt.Errorf("Create root directory failed | rootdir=%v | err=[%w]", rootdir, err)
	}

	return &Server{
		logger:      logging.New(logLevel),
		rootdir:     rootdir,
		flushThresh: flushThresh,
		blockSize:   blockSize,
		initDone:    false,
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
	s.raftEngine, err = raft.NewEngine(s.logger.Level(), s.rootdir, s.clusterMeta)
	if err != nil {
		return fmt.Errorf("Create raft engine failed | rootdir=%v | err=[%w]", s.rootdir, err)
	}

	// create shard manager
	s.shardMgr = shardmgr.New(s.logger.Level(), s.rootdir, s.flushThresh, s.blockSize, s.raftEngine)

	// signal init complete
	s.setInitDone()

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

func (s *Server) setInitDone() {
	s.initDoneLock.Lock()
	defer s.initDoneLock.Unlock()
	s.initDone = true
}

func (s *Server) isInitDone() bool {
	s.initDoneLock.Lock()
	defer s.initDoneLock.Unlock()
	return s.initDone
}

// -------------------------------
// pb.StorageServer Implementation
// -------------------------------

// Get returns the value associated with the key, and a boolean indicating whether the key exists.
func (s *Server) Get(ctx context.Context, req *pb.GetReq) (*pb.GetResp, error) {
	s.logger.Debug("Get | key=%v", store.Key(req.GetKey()))

	if !s.isInitDone() {
		return nil, ErrNotReady
	}

	if ctx.Err() == context.Canceled {
		return nil, ErrCanceled
	}

	val, found, err := s.shardMgr.Get(req.GetKey())
	if err != nil {
		s.logger.Error("Get key failed | key=%v | err=[%v]", store.Key(req.GetKey()), err)
		return nil, ErrInternal
	}

	return &pb.GetResp{
		Val:   []byte(val),
		Found: found,
	}, nil
}

// Put adds or updates a key-value pair to the storage.
func (s *Server) Put(ctx context.Context, req *pb.PutReq) (*pb.PutResp, error) {
	s.logger.Debug("Put | key=%v | val=%v", store.Key(req.GetKey()), store.Value(req.GetVal()))

	if !s.isInitDone() {
		return nil, ErrNotReady
	}

	if ctx.Err() == context.Canceled {
		return nil, ErrCanceled
	}

	if !s.raftEngine.IsLeader() {
		leaderID := s.raftEngine.LeaderID()
		if leaderID == s.clusterMeta.NodeIDNil() {
			return nil, ErrLeaderNotFound
		}
		return s.redirectPut(leaderID, req)
	}

	err := s.shardMgr.Put(req.GetKey(), req.GetVal())
	if err != nil {
		s.logger.Error("Put key failed | key=%v | val=%v | err=[%v]",
			store.Key(req.GetKey()), store.Value(req.GetVal()), err)
		return nil, ErrInternal
	}

	return &pb.PutResp{}, nil
}

// Del removes key from the storage.
func (s *Server) Del(ctx context.Context, req *pb.DelReq) (*pb.DelResp, error) {
	s.logger.Debug("Del | key=%v", store.Key(req.GetKey()))

	if !s.isInitDone() {
		return nil, ErrNotReady
	}

	if ctx.Err() == context.Canceled {
		return nil, ErrCanceled
	}

	if !s.raftEngine.IsLeader() {
		leaderID := s.raftEngine.LeaderID()
		if leaderID == s.clusterMeta.NodeIDNil() {
			return nil, ErrLeaderNotFound
		}
		return s.redirectDel(leaderID, req)
	}

	err := s.shardMgr.Del(req.GetKey())
	if err != nil {
		s.logger.Error("Delete key failed | err=[%v]", err)
		return nil, ErrInternal
	}

	return &pb.DelResp{}, nil
}

func (s *Server) redirectPut(id int32, req *pb.PutReq) (*pb.PutResp, error) {
	resp, err := s.clusterMeta.StorageClient(id).Put(context.Background(), req)
	s.logger.Debug("Redirected put request | key=%v | val=%v | err=[%v]",
		store.Key(req.GetKey()), store.Value(req.GetVal()), err)
	return resp, err
}

func (s *Server) redirectDel(id int32, req *pb.DelReq) (*pb.DelResp, error) {
	resp, err := s.clusterMeta.StorageClient(id).Del(context.Background(), req)
	s.logger.Debug("Redirected delete request | key=%v | err=[%v]", store.Key(req.GetKey()), err)
	return resp, err
}

// ----------------------------
// pb.RaftServer Implementation
// ----------------------------

// RequestVote invoked by candidates to gather votes.
func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {

	if !s.isInitDone() {
		return nil, ErrNotReady
	}

	if ctx.Err() == context.Canceled {
		return nil, ErrCanceled
	}

	return s.raftEngine.RequestVoteHandler(req), nil
}

// AppendEntries invoked by leader to replicate log entries, also used as heartbeat.
func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesReq) (*pb.AppendEntriesResp, error) {

	if !s.isInitDone() {
		return nil, ErrNotReady
	}

	if ctx.Err() == context.Canceled {
		return nil, ErrCanceled
	}

	return s.raftEngine.AppendEntriesHandler(req), nil
}
