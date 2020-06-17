package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"google.golang.org/grpc"

	"github.com/chuyangliu/rawkv/pkg/cluster"
	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/pb"
)

const (
	dirRaft         string = "raft"
	fileCurrentTerm string = "currentTerm"
	fileVotedFor    string = "votedFor"
	fileLogs        string = "logs"

	roleFollower  uint8 = 0
	roleCandidate uint8 = 1
	roleLeader    uint8 = 2

	nodeIDNil int32 = -1
)

// Engine manages Raft states and operations.
type Engine struct {
	raftdir string
	logger  *logging.Logger

	// cluster info
	nodeProvider cluster.NodeProvider
	clusterSize  int32
	nodeID       int32

	// persistent state on all servers
	currentTerm uint64
	votedFor    int32
	logs        []*raftLog // log index starts at 1

	// volatile state on all servers
	commitIndex uint64
	lastApplied uint64

	// volatile state on all leaders
	nextIndex  []uint64
	matchIndex []uint64

	// leader or follower or candidate
	role uint8

	// leader node id
	leaderID int32

	// election timer
	electionTimer *raftTimer
}

// NewEngine instantiates an Engine.
func NewEngine(rootdir string, logLevel int) (*Engine, error) {

	// create directory to store Raft states
	raftdir := path.Join(rootdir, dirRaft)
	if err := os.MkdirAll(raftdir, 0777); err != nil {
		return nil, fmt.Errorf("Create root directory failed | raftdir=%v | err=[%w]", raftdir, err)
	}

	// create node provider
	nodeProvider, err := cluster.NewK8SNodeProvider(logLevel)
	if err != nil {
		return nil, fmt.Errorf("Create Kubernetes node provider failed | raftdir=%v | err=[%w]", raftdir, err)
	}

	// get number of Raft nodes in the cluster
	clusterSize, err := nodeProvider.Size()
	if err != nil {
		return nil, fmt.Errorf("Query cluster size failed | raftdir=%v | err=[%w]", raftdir, err)
	}
	if clusterSize < 3 {
		return nil, fmt.Errorf("Require at least three nodes in the cluster for fault tolerance | raftdir=%v"+
			" | clusterSize=%v | err=[%w]", raftdir, clusterSize, err)
	}

	// get current node index in the cluster
	nodeID, err := nodeProvider.ID()
	if err != nil {
		return nil, fmt.Errorf("Query node id failed | raftdir=%v | clusterSize=%v | err=[%w]",
			raftdir, clusterSize, err)
	}

	// instantiate
	engine := &Engine{
		raftdir: raftdir,
		logger:  logging.New(logLevel),

		nodeProvider: nodeProvider,
		clusterSize:  clusterSize,
		nodeID:       nodeID,

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]uint64, clusterSize),
		matchIndex: make([]uint64, clusterSize),

		role:     roleFollower,
		leaderID: nodeIDNil,

		electionTimer: newRaftTimer(logLevel, 2000, 5000),
	}

	// nextIndex starts at 1
	for i := range engine.nextIndex {
		engine.nextIndex[i] = 1
	}

	// init Raft persistent states
	if err := engine.initPersistStates(); err != nil {
		return nil, fmt.Errorf("Initialize Raft persistent states failed | raftdir=%v | clusterSize=%v | nodeID=%v"+
			" | err=[%w]", raftdir, clusterSize, nodeID, err)
	}

	return engine, nil
}

func (e *Engine) initPersistStates() error {

	// init currentTerm
	filePath := path.Join(e.raftdir, fileCurrentTerm)
	if err := e.initCurrentTerm(filePath); err != nil {
		return fmt.Errorf("Initialize currentTerm failed | filePath=%v | states=%v | err=[%w]", filePath, e, err)
	}

	// init votedFor
	filePath = path.Join(e.raftdir, fileVotedFor)
	if err := e.initVotedFor(filePath); err != nil {
		return fmt.Errorf("Initialize votedFor failed | filePath=%v | states=%v | err=[%w]", filePath, e, err)
	}

	// init logs
	filePath = path.Join(e.raftdir, fileLogs)
	if err := e.initLogs(filePath); err != nil {
		return fmt.Errorf("Initialize logs failed | filePath=%v | states=%v | err=[%w]", filePath, e, err)
	}

	return nil
}

func (e *Engine) initCurrentTerm(filePath string) error {

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("Open currentTerm file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
	}
	defer file.Close()

	if err := binary.Read(file, binary.BigEndian, &e.currentTerm); err != nil {
		if err == io.EOF {
			e.currentTerm = 0
			if err = binary.Write(file, binary.BigEndian, e.currentTerm); err != nil {
				return fmt.Errorf("Write initial currentTerm file failed | path=%v | states=%v | err=[%w]",
					filePath, e, err)
			}
		} else {
			return fmt.Errorf("Read currentTerm file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
		}
	}

	return nil
}

func (e *Engine) initVotedFor(filePath string) error {

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("Open votedFor file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
	}
	defer file.Close()

	if err := binary.Read(file, binary.BigEndian, &e.votedFor); err != nil {
		if err == io.EOF {
			e.votedFor = nodeIDNil
			if err = binary.Write(file, binary.BigEndian, e.votedFor); err != nil {
				return fmt.Errorf("Write initial votedFor file failed | path=%v | states=%v | err=[%w]",
					filePath, e, err)
			}
		} else {
			return fmt.Errorf("Read votedFor file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
		}
	}

	return nil
}

func (e *Engine) initLogs(filePath string) error {

	file, err := os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("Open logs file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
	}
	defer file.Close()

	// add a dummy log to make log index start at 1
	e.logs = []*raftLog{{index: 0, term: 0}}

	for {
		log, err := readLog(file)
		if err != nil {
			return fmt.Errorf("Read log failed | path=%v | states=%v | err=[%w]", filePath, e, err)
		}
		if log == nil {
			break
		}
		e.logs = append(e.logs, log)
	}

	return nil
}

func (e *Engine) String() string {
	return fmt.Sprintf("[raftdir=%v | clusterSize=%v | nodeID=%v | currentTerm=%v | votedFor=%v | logSize=%v"+
		" | commitIndex=%v | lastApplied=%v | nextIndex=%v | matchIndex=%v | role=%v | leaderID=%v]", e.raftdir,
		e.clusterSize, e.nodeID, e.currentTerm, e.votedFor, len(e.logs)-1, e.commitIndex, e.lastApplied, e.nextIndex,
		e.matchIndex, e.role, e.leaderID)
}

// Run starts Raft service on current node.
func (e *Engine) Run() {
	e.logger.Info("Raft engine started | states=%v", e)
	for {
		switch e.role {
		case roleFollower:
			e.follower()
		case roleCandidate:
			e.candidate()
		case roleLeader:
			e.leader()
		default:
			panic(fmt.Sprintf("Invalid raft role (this error most likely comes from internal raft implementation)"+
				" | states=%v", e))
		}
		e.logger.Debug("Print raft states | states=%v", e)
		time.Sleep(5 * time.Second) // TODO remove later
	}
}

// RequestVoteHandler handles RequestVote RPC request.
func (e *Engine) RequestVoteHandler(req *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {
	grantVote := false

	if req.Term >= e.currentTerm {
		// TODO grant vote when logs at least up-to-date
		grantVote = (e.votedFor == nodeIDNil || e.votedFor == req.CandidateID)
		if req.Term > e.currentTerm {
			e.role = roleFollower
		}
	}

	if grantVote && e.votedFor == nodeIDNil {
		if err := e.vote(req.CandidateID); err != nil {
			e.logger.Error("Vote candidate failed | req=%v | err=[%v]", req, err)
			grantVote = false
		}
	}

	resp := &pb.RequestVoteResp{
		Term:        e.currentTerm,
		VoteGranted: grantVote,
	}

	e.logger.Debug("RequestVote handled | req=%v | resp=%v | states=%v", req, resp, e)
	return resp, nil
}

// AppendEntriesHandler handles AppendEntries RPC request.
func (e *Engine) AppendEntriesHandler(req *pb.AppendEntriesReq) (*pb.AppendEntriesResp, error) {
	// TODO AppendEntries handler
	resp := &pb.AppendEntriesResp{}
	e.logger.Debug("AppendEntries handled | req=%v | resp=%v | states=%v", req, resp, e)
	return resp, nil
}

func (e *Engine) follower() {
	e.electionTimer.start()
	select {
	case <-e.electionTimer.timeout():
		e.role = roleCandidate
		return
		// TODO wait append entry channel
	}
}

func (e *Engine) candidate() {

	// increate current term
	if err := e.incrCurrentTerm(); err != nil {
		e.logger.Error("Increase current term failed | states=%v | err=[%v]", e, err)
		return
	}

	// vote for self
	if err := e.vote(e.nodeID); err != nil {
		e.logger.Error("Vote for self failed | states=%v | err=[%v]", e, err)
		return
	}

	// start election timer
	e.electionTimer.start()

	// send RequestVote RPCs to all other servers
	majority := make(chan bool)
	e.requestVotesAsync(majority)

	// wait timeout or vote replies
	select {
	case <-e.electionTimer.timeout():
		return
	case suc := <-majority:
		e.electionTimer.stop()
		if suc {
			e.leaderID = e.votedFor
			e.role = roleLeader
			e.logger.Info("New leader elected | states=%v", e)
		}
	}
}

func (e *Engine) incrCurrentTerm() error {

	filePath := path.Join(e.raftdir, fileCurrentTerm)
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("Open currentTerm file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
	}
	defer file.Close()

	if err = binary.Write(file, binary.BigEndian, e.currentTerm+1); err != nil {
		return fmt.Errorf("Write currentTerm file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
	}

	e.currentTerm++
	return nil
}

func (e *Engine) vote(nodeID int32) error {

	filePath := path.Join(e.raftdir, fileVotedFor)
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("Open votedFor file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
	}
	defer file.Close()

	if err = binary.Write(file, binary.BigEndian, nodeID); err != nil {
		return fmt.Errorf("Write votedFor file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
	}

	e.votedFor = nodeID
	return nil
}

func (e *Engine) requestVotesAsync(majority chan bool) {
	go func() {

		votes := make(chan bool)
		for i := int32(0); i < e.clusterSize; i++ {
			if i != e.nodeID {
				e.requestVoteAsync(i, votes)
			}
		}

		numGranted := int32(0)
		for i := int32(0); i < e.clusterSize-1; i++ {
			if <-votes {
				numGranted++
			}
		}

		// no need to add 1 when counting majority since current node votes for self in advance
		majority <- (numGranted >= e.clusterSize/2)
	}()
}

func (e *Engine) requestVoteAsync(targetID int32, votes chan bool) {
	go func() {

		targetAddr, err := e.nodeProvider.RaftAddr(targetID)
		if err != nil {
			e.logger.Error("Get node address failed | targetID=%v | states=%v | err=[%v]", targetID, e, err)
			votes <- false
			return
		}

		conn, err := grpc.Dial(targetAddr, grpc.WithInsecure())
		if err != nil {
			e.logger.Error("Connect raft server failed | targetAddr=%v | states=%v | err=[%v]", targetAddr, e, err)
			votes <- false
			return
		}
		defer conn.Close()

		lastLog := e.logs[len(e.logs)-1]
		req := &pb.RequestVoteReq{
			Term:         e.currentTerm,
			CandidateID:  e.nodeID,
			LastLogIndex: lastLog.index,
			LastLogTerm:  lastLog.term,
		}

		resp, err := pb.NewRaftClient(conn).RequestVote(context.Background(), req)
		if err != nil {
			e.logger.Error("RequestVote failed | req=%v | targetAddr=%v | states=%v | err=[%v]",
				req, targetAddr, e, err)
			votes <- false
			return
		}
		e.logger.Debug("RequestVote sent | req=%v | resp=%v | targetAddr=%v | states=%v", req, resp, targetAddr, e)

		if resp.GetTerm() > e.currentTerm {
			e.role = roleFollower
			votes <- false
		} else {
			votes <- resp.VoteGranted
		}
	}()
}

func (e *Engine) leader() {
	// TODO
}
