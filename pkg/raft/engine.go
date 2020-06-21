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

// ApplyLogFunc applies a raft log to state machine.
type ApplyLogFunc func(cmd uint32, rawKey []byte, rawVal []byte) error

// Engine manages raft states and operations.
type Engine struct {
	raftdir  string
	applyLog ApplyLogFunc
	logger   *logging.Logger

	// cluster info
	nodeProvider cluster.NodeProvider
	clusterSize  int32
	nodeID       int32
	clients      []pb.RaftClient

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

	// time interval (milliseconds) between two heartbeats (AppendEntries RPCs) sent from leader
	heartbeatInterval int64

	// channel to cancel follower's election timeout
	cancelElectionTimeout chan bool
}

// NewEngine instantiates an Engine.
func NewEngine(rootdir string, applyLog ApplyLogFunc, logLevel int) (*Engine, error) {

	// create directory to store raft states
	raftdir := path.Join(rootdir, dirRaft)
	if err := os.MkdirAll(raftdir, 0777); err != nil {
		return nil, fmt.Errorf("Create root directory failed | raftdir=%v | err=[%w]", raftdir, err)
	}

	// create node provider
	nodeProvider, err := cluster.NewK8SNodeProvider(logLevel)
	if err != nil {
		return nil, fmt.Errorf("Create Kubernetes node provider failed | raftdir=%v | err=[%w]", raftdir, err)
	}

	// get number of raft nodes in the cluster
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

	// establish grpc connections with other raft nodes in the cluster
	clients, err := createRaftClients(nodeProvider, clusterSize, nodeID)
	if err != nil {
		return nil, fmt.Errorf("Create raft clients failed | raftdir=%v | clusterSize=%v | nodeID=%v | err=[%w]",
			raftdir, clusterSize, nodeID, err)
	}

	// instantiate
	engine := &Engine{
		raftdir:  raftdir,
		applyLog: applyLog,
		logger:   logging.New(logLevel),

		nodeProvider: nodeProvider,
		clusterSize:  clusterSize,
		nodeID:       nodeID,
		clients:      clients,

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]uint64, clusterSize),
		matchIndex: make([]uint64, clusterSize),

		role:     roleFollower,
		leaderID: nodeIDNil,

		electionTimer:     newRaftTimer(logLevel, 2000, 5000),
		heartbeatInterval: 1000,

		cancelElectionTimeout: make(chan bool),
	}

	// nextIndex starts at 1
	for i := range engine.nextIndex {
		engine.nextIndex[i] = 1
	}

	// init raft persistent states
	if err := engine.initPersistStates(); err != nil {
		return nil, fmt.Errorf("Initialize raft persistent states failed | err=[%w]", err)
	}

	return engine, nil
}

func createRaftClients(nodeProvider cluster.NodeProvider, clusterSize int32, nodeID int32) ([]pb.RaftClient, error) {
	clients := make([]pb.RaftClient, clusterSize)

	for id := int32(0); id < clusterSize; id++ {
		if id != nodeID {

			targetAddr, err := nodeProvider.RaftAddr(id)
			if err != nil {
				return nil, fmt.Errorf("Get node address failed | targetID=%v | clusterSize=%v | nodeID=%v | err=[%w]",
					id, clusterSize, nodeID, err)
			}

			conn, err := grpc.Dial(targetAddr, grpc.WithInsecure())
			if err != nil {
				return nil, fmt.Errorf("Connect raft server failed | targetAddr=%v | clusterSize=%v | nodeID=%v"+
					" | err=[%w]", targetAddr, clusterSize, nodeID, err)
			}

			clients[id] = pb.NewRaftClient(conn)
		}
	}

	return clients, nil
}

func (e *Engine) initPersistStates() error {

	if err := e.initCurrentTerm(path.Join(e.raftdir, fileCurrentTerm)); err != nil {
		return fmt.Errorf("Initialize currentTerm failed | err=[%w]", err)
	}

	if err := e.initVotedFor(path.Join(e.raftdir, fileVotedFor)); err != nil {
		return fmt.Errorf("Initialize votedFor failed | err=[%w]", err)
	}

	if err := e.initLogs(path.Join(e.raftdir, fileLogs)); err != nil {
		return fmt.Errorf("Initialize logs failed | err=[%w]", err)
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
	e.logs = []*raftLog{{
		entry: &pb.AppendEntriesReq_LogEntry{
			Index: 0,
			Term:  0,
		},
	}}

	for {
		log, err := newRaftLogFromFile(file)
		if err != nil {
			return fmt.Errorf("Read log failed | path=%v | states=%v | err=[%w]", filePath, e, err)
		}
		if log == nil { // EOF
			break
		}
		e.logs = append(e.logs, log)
	}

	return nil
}

func (e *Engine) String() string {
	return fmt.Sprintf("[raftdir=%v | clusterSize=%v | nodeID=%v | currentTerm=%v | votedFor=%v | logs=%v"+
		" | commitIndex=%v | lastApplied=%v | nextIndex=%v | matchIndex=%v | role=%v | leaderID=%v]", e.raftdir,
		e.clusterSize, e.nodeID, e.currentTerm, e.votedFor, e.logs, e.commitIndex, e.lastApplied, e.nextIndex,
		e.matchIndex, e.role, e.leaderID)
}

// Run starts raft service on current node.
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
			panic(fmt.Sprintf("Invalid raft role (error most likely caused by incorrect raft implementation)"+
				" | states=%v", e))
		}
		e.logger.Debug("Print raft states | states=%v", e)
	}
}

// RequestVoteHandler handles received RequestVote RPC.
func (e *Engine) RequestVoteHandler(req *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {

	resp := &pb.RequestVoteResp{
		Term:        e.currentTerm,
		VoteGranted: false,
	}

	if req.GetTerm() < e.currentTerm {
		e.logger.Info("RequestVote received from older term | req=%v | states=%v", req, e)
		return resp, nil
	}

	if req.GetTerm() > e.currentTerm {
		if err := e.convertToFollower(req.GetTerm()); err != nil {
			e.logger.Error("Convert to follower failed | err=[%v]", err)
			return resp, nil
		}
	}

	if (e.votedFor == nodeIDNil || e.votedFor == req.CandidateID) &&
		e.atLeastUpToDate(req.LastLogIndex, req.LastLogTerm) {
		if err := e.vote(req.CandidateID); err != nil {
			e.logger.Error("Vote candidate failed | req=%v | err=[%v]", req, err)
			return resp, nil
		}
		resp.VoteGranted = true
		e.cancelElectionTimeout <- true
	}

	e.logger.Debug("RequestVote handled | req=%v | resp=%v | states=%v", req, resp, e)
	return resp, nil
}

func (e *Engine) atLeastUpToDate(lastLogIndex uint64, lastLogTerm uint64) bool {
	lastLog := e.logs[len(e.logs)-1]
	if lastLogTerm == lastLog.entry.Term {
		return lastLogIndex >= lastLog.entry.Index
	}
	return lastLogTerm >= lastLog.entry.Term
}

// AppendEntriesHandler handles received AppendEntries RPC.
func (e *Engine) AppendEntriesHandler(req *pb.AppendEntriesReq) (*pb.AppendEntriesResp, error) {

	resp := &pb.AppendEntriesResp{
		Term:    e.currentTerm,
		Success: false,
	}

	if req.GetTerm() < e.currentTerm {
		e.logger.Info("AppendEntries received from older term | req=%v | states=%v", req, e)
		return resp, nil
	}

	if req.GetTerm() > e.currentTerm || e.role == roleCandidate {
		if err := e.convertToFollower(req.GetTerm()); err != nil {
			e.logger.Error("Convert to follower failed | err=[%v]", err)
			return resp, nil
		}
	}

	if e.role == roleLeader {
		panic(fmt.Sprintf("More than one leader exists in the cluster (error most likely caused by incorrect raft"+
			" implementation) | req=%v | states=%v", req, e))
	}

	e.leaderID = req.GetLeaderID()
	e.cancelElectionTimeout <- true

	prevLogIndex := req.GetPrevLogIndex()
	if prevLogIndex >= uint64(len(e.logs)) || e.logs[prevLogIndex].entry.Term != req.GetPrevLogTerm() {
		e.logger.Info("AppendEntries logs did not match | req=%v | states=%v", req, e)
		return resp, nil
	}

	// TODO update logs on disk
	index := prevLogIndex + 1
	for _, entry := range req.GetEntries() {
		if entry.Index != index {
			panic(fmt.Sprintf("Incorrect AppendEntries log index (error most likely caused by incorrect raft"+
				" implementation) | req=%v | states=%v", req, e))
		}
		if index >= uint64(len(e.logs)) {
			e.logs = append(e.logs, newRaftLog(entry))
		} else {
			if entry.Term != e.logs[index].entry.Term {
				// conflict entry (same index but different terms), delete the existing entry and all that follow it
				e.logs = e.logs[:index+1]
			}
			e.logs[index] = newRaftLog(entry)
		}
		index++
	}

	if req.GetLeaderCommit() > e.commitIndex {
		e.commitIndex = min(req.GetLeaderCommit(), uint64(len(e.logs)-1))
		if err := e.apply(); err != nil {
			e.logger.Error("Apply failed | err=[%w]", err)
			return resp, nil
		}
	}

	resp.Success = true
	e.logger.Debug("AppendEntries handled | req=%v | resp=%v | states=%v", req, resp, e)
	return resp, nil
}

func (e *Engine) follower() {
	e.electionTimer.start()
	select {
	case <-e.electionTimer.timeout():
		e.role = roleCandidate
	case <-e.cancelElectionTimeout:
		e.electionTimer.stop()
	}
}

func (e *Engine) candidate() {

	if err := e.incrCurrentTerm(); err != nil {
		e.logger.Error("Increase current term failed | err=[%v]", err)
		return
	}

	if err := e.vote(e.nodeID); err != nil {
		e.logger.Error("Vote for self failed | err=[%v]", err)
		return
	}

	e.electionTimer.start()
	majority := make(chan bool)
	e.requestVotesAsync(majority)

	select {
	case <-e.electionTimer.timeout():
		return
	case suc := <-majority:
		if suc {
			e.electionTimer.stop()
			e.role = roleLeader
			e.leaderID = e.nodeID
			e.logger.Info("New leader elected | states=%v", e)
		} else {
			// not receive majority, wait timeout before starting new election
			<-e.electionTimer.timeout()
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
		for id := int32(0); id < e.clusterSize; id++ {
			if id != e.nodeID {
				e.requestVoteAsync(id, votes)
			}
		}

		numGrant := int32(1) // start at 1 since candidate votes for self before requesting votes to others
		for id := int32(0); id < e.clusterSize; id++ {
			if id != e.nodeID && <-votes {
				numGrant++
			}
		}

		majority <- e.isMajority(numGrant)
	}()
}

func (e *Engine) requestVoteAsync(targetID int32, votes chan bool) {
	go func() {

		lastLog := e.logs[len(e.logs)-1]
		req := &pb.RequestVoteReq{
			Term:         e.currentTerm,
			CandidateID:  e.nodeID,
			LastLogIndex: lastLog.entry.Index,
			LastLogTerm:  lastLog.entry.Term,
		}

		resp, err := e.clients[targetID].RequestVote(context.Background(), req)
		if err != nil {
			e.logger.Error("RequestVote failed | states=%v | err=[%v]", e, err)
			votes <- false
			return
		}
		e.logger.Debug("RequestVote sent | targetID=%v | req=%v | resp=%v | states=%v", targetID, req, resp, e)

		if resp.GetTerm() > e.currentTerm {
			if err := e.convertToFollower(resp.GetTerm()); err != nil {
				e.logger.Error("Convert to follower failed | err=[%v]", err)
			}
		}

		votes <- resp.VoteGranted
	}()
}

func (e *Engine) leader() {
	exits := make(chan bool)
	for id := int32(0); id < e.clusterSize; id++ {
		if id != e.nodeID {
			e.appendEntriesAsync(id, exits)
		}
	}
	for id := int32(0); id < e.clusterSize; id++ {
		if id != e.nodeID {
			<-exits
		}
	}
}

func (e *Engine) appendEntriesAsync(targetID int32, exits chan bool) {
	go func() {
		for e.role == roleLeader {
			nextIndex := e.nextIndex[targetID]
			prevLog := e.logs[nextIndex-1]
			req := &pb.AppendEntriesReq{
				Term:         e.currentTerm,
				LeaderID:     e.nodeID,
				PrevLogIndex: prevLog.entry.Index,
				PrevLogTerm:  prevLog.entry.Term,
				Entries:      make([]*pb.AppendEntriesReq_LogEntry, 0),
				LeaderCommit: e.commitIndex,
			}

			lastLogIndex := uint64(len(e.logs) - 1)
			for ; nextIndex <= lastLogIndex; nextIndex++ {
				req.Entries = append(req.Entries, e.logs[nextIndex].entry)
			}

			resp, err := e.clients[targetID].AppendEntries(context.Background(), req)
			if err != nil {
				e.logger.Error("AppendEntries failed | states=%v | err=[%v]", e, err)
				return
			}
			e.logger.Debug("AppendEntries sent | targetID=%v | req=%v | resp=%v | states=%v", targetID, req, resp, e)

			if resp.GetTerm() > e.currentTerm {
				if err := e.convertToFollower(resp.GetTerm()); err != nil {
					e.logger.Error("Convert to follower failed | err=[%v]", err)
				}
				break
			}

			if len(req.Entries) > 0 {
				if resp.GetSuccess() {
					e.matchIndex[targetID] = nextIndex
					e.nextIndex[targetID]++
					if err := e.commit(nextIndex); err != nil {
						e.logger.Error("Commit failed | err=[%v]", err)
					}
				} else {
					e.nextIndex[targetID] = max(1, e.nextIndex[targetID]-1)
				}
			}

			e.waitHeartbeatInterval()
		}
		exits <- true
	}()
}

func (e *Engine) commit(logIndex uint64) error {
	if logIndex > e.commitIndex && e.logs[logIndex].entry.Term == e.currentTerm {
		numMatch := int32(1) // log[logIndex] already replicated on current node
		for id := int32(0); id < e.clusterSize; id++ {
			if id != e.nodeID && e.matchIndex[id] >= logIndex {
				numMatch++
			}
		}
		if e.isMajority(numMatch) {
			e.commitIndex = logIndex
			if err := e.apply(); err != nil {
				return fmt.Errorf("Apply failed | err=[%w]", err)
			}
		}
	}
	return nil
}

func (e *Engine) apply() error {
	for ; e.commitIndex > e.lastApplied; e.lastApplied++ {
		log := e.logs[e.lastApplied+1].entry
		if err := e.applyLog(log.Cmd, log.Key, log.Val); err != nil {
			return fmt.Errorf("Apply log failed | log=%v | states=%v | err=[%w]", log, e, err)
		}
	}
	return nil
}

func (e *Engine) convertToFollower(newCurrentTerm uint64) error {
	if err := e.vote(nodeIDNil); err != nil {
		return fmt.Errorf("Reset votedFor failed | err=[%w]", err)
	}
	e.currentTerm = newCurrentTerm
	e.role = roleFollower
	return nil
}

func (e *Engine) waitHeartbeatInterval() {
	time.Sleep(time.Duration(e.heartbeatInterval) * time.Millisecond)
}

func (e *Engine) isMajority(num int32) bool {
	return num > e.clusterSize/2
}

func min(a uint64, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

func max(a uint64, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}
