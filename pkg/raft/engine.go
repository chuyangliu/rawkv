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

	engine := &Engine{
		raftdir:  raftdir,
		applyLog: applyLog,
		logger:   logging.New(logLevel),
	}

	if err := engine.init(); err != nil {
		return nil, fmt.Errorf("Initialize raft engine failed | err=[%w]", err)
	}

	return engine, nil
}

func (e *Engine) String() string {
	return fmt.Sprintf("[raftdir=%v | clusterSize=%v | nodeID=%v | currentTerm=%v | votedFor=%v | logs=%v"+
		" | commitIndex=%v | lastApplied=%v | nextIndex=%v | matchIndex=%v | role=%v | leaderID=%v]", e.raftdir,
		e.clusterSize, e.nodeID, e.currentTerm, e.votedFor, e.logs, e.commitIndex, e.lastApplied, e.nextIndex,
		e.matchIndex, e.role, e.leaderID)
}

func (e *Engine) init() error {
	var err error

	// create node provider
	e.nodeProvider, err = cluster.NewK8SNodeProvider(e.logger.Level())
	if err != nil {
		return fmt.Errorf("Create k8s node provider failed | states=%v | err=[%w]", e, err)
	}

	// get number of raft nodes in the cluster, wait until cluster size >= 3
	for {
		e.clusterSize, err = e.nodeProvider.Size()
		if err != nil {
			return fmt.Errorf("Get cluster size failed | states=%v | err=[%w]", e, err)
		}
		if e.clusterSize >= 3 {
			break
		}
		e.logger.Warn("Require at least three nodes in the cluster for fault tolerance. Retry after 1 second"+
			" | states=%v", e)
		time.Sleep(1 * time.Second)
	}

	// get current node index in the cluster
	e.nodeID, err = e.nodeProvider.ID()
	if err != nil {
		return fmt.Errorf("Get node id failed | states=%v | err=[%w]", e, err)
	}

	// init persistent state: currentTerm
	if err := e.initCurrentTerm(path.Join(e.raftdir, fileCurrentTerm)); err != nil {
		return fmt.Errorf("Initialize currentTerm failed | err=[%w]", err)
	}

	// init persistens state: votedFor
	if err := e.initVotedFor(path.Join(e.raftdir, fileVotedFor)); err != nil {
		return fmt.Errorf("Initialize votedFor failed | err=[%w]", err)
	}

	// init persistent state: logs
	if err := e.initLogs(path.Join(e.raftdir, fileLogs)); err != nil {
		return fmt.Errorf("Initialize logs failed | err=[%w]", err)
	}

	// init volatile states on all servers
	e.commitIndex = 0
	e.lastApplied = 0

	// init volatile states on leaders
	e.matchIndex = make([]uint64, e.clusterSize)
	e.nextIndex = make([]uint64, e.clusterSize)
	for i := range e.nextIndex {
		e.nextIndex[i] = 1
	}

	// init metadata
	e.role = roleFollower
	e.leaderID = e.nodeProvider.IDNil()

	// init timers and timeouts
	e.electionTimer = newRaftTimer(e.logger.Level(), 5000, 10000)
	e.heartbeatInterval = 3000

	// init channels
	e.cancelElectionTimeout = make(chan bool)

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
			e.votedFor = e.nodeProvider.IDNil()
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

// Run starts raft service on current node.
func (e *Engine) Run() error {

	// establish grpc connections with other raft nodes in the cluster
	if err := e.initRaftClients(); err != nil {
		return fmt.Errorf("Create grpc clients failed | err=[%w]", err)
	}

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
	}
}

func (e *Engine) initRaftClients() error {
	e.clients = make([]pb.RaftClient, e.clusterSize)

	for id := int32(0); id < e.clusterSize; id++ {
		if id != e.nodeID {

			targetAddr, err := e.nodeProvider.RaftAddr(id)
			if err != nil {
				return fmt.Errorf("Get node address failed | targetID=%v | states=%v | err=[%w]", id, e, err)
			}

			conn, err := grpc.Dial(targetAddr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				return fmt.Errorf("Connect raft server failed | targetAddr=%v | states=%v | err=[%w]",
					targetAddr, e, err)
			}

			e.clients[id] = pb.NewRaftClient(conn)
		}
	}

	return nil
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
		e.logger.Info("RequestVote received from newer term: convert to follower | req=%v | states=%v", req, e)
		if err := e.convertToFollower(req.GetTerm()); err != nil {
			e.logger.Error("Convert to follower failed | err=[%v]", err)
			return resp, nil
		}
	}

	if (e.votedFor == e.nodeProvider.IDNil() || e.votedFor == req.CandidateID) &&
		e.atLeastUpToDate(req.LastLogIndex, req.LastLogTerm) {
		if err := e.setVotedFor(req.CandidateID); err != nil {
			e.logger.Error("Vote candidate failed | req=%v | err=[%v]", req, err)
			return resp, nil
		}
		resp.VoteGranted = true
		e.cancelElectionTimeout <- true
	}

	e.logger.Info("RequestVote handled | req=%v | resp=%v | states=%v", req, resp, e)
	return resp, nil
}

func (e *Engine) atLeastUpToDate(lastLogIndex uint64, lastLogTerm uint64) bool {
	lastLog := e.logs[len(e.logs)-1]
	if lastLogTerm == lastLog.entry.GetTerm() {
		return lastLogIndex >= lastLog.entry.GetIndex()
	}
	return lastLogTerm >= lastLog.entry.GetTerm()
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
		e.logger.Info("AppendEntries received from newer term: convert to follower | req=%v | states=%v", req, e)
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
	if prevLogIndex >= uint64(len(e.logs)) || e.logs[prevLogIndex].entry.GetTerm() != req.GetPrevLogTerm() {
		e.logger.Info("AppendEntries logs did not match | req=%v | states=%v", req, e)
		return resp, nil
	}

	index := prevLogIndex + 1
	for _, entry := range req.GetEntries() {
		if entry.GetIndex() != index {
			panic(fmt.Sprintf("Incorrect AppendEntries log index (error most likely caused by incorrect raft"+
				" implementation) | req=%v | states=%v", req, e))
		}
		if index >= uint64(len(e.logs)) {
			if err := e.appendLog(newRaftLog(entry)); err != nil {
				e.logger.Error("Append log failed | err=[%v]", err)
				return resp, nil
			}
		} else {
			if entry.GetTerm() != e.logs[index].entry.GetTerm() {
				// conflict entry (same index but different terms), delete the existing entry and all that follow it
				if err := e.truncateAndAppendLog(newRaftLog(entry)); err != nil {
					e.logger.Error("Truncate and append log failed | err=[%v]", err)
					return resp, nil
				}
			}
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

func (e *Engine) appendLog(log *raftLog) error {

	filePath := path.Join(e.raftdir, fileLogs)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("Open logs file failed | log=%v | path=%v | states=%v | err=[%w]", log, filePath, e, err)
	}
	defer file.Close()

	// append
	if err := log.write(file); err != nil {
		return fmt.Errorf("Write log file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
	}

	e.logs = append(e.logs, log)
	return nil
}

func (e *Engine) truncateAndAppendLog(log *raftLog) error {

	filePath := path.Join(e.raftdir, fileLogs)
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("Open logs file failed | log=%v | path=%v | states=%v | err=[%w]", log, filePath, e, err)
	}
	defer file.Close()

	// compute file size to retain after truncate
	size := int64(0)
	for i := uint64(1); i < log.entry.GetIndex(); i++ {
		size += e.logs[i].size()
	}

	// truncate
	if err := file.Truncate(size); err != nil {
		return fmt.Errorf("Truncate log file failed | log=%v | path=%v | states=%v | err=[%w]", log, filePath, e, err)
	}

	// seek to file end
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("Seek log file failed | log=%v | path=%v | states=%v | err=[%w]", log, filePath, e, err)
	}

	// append
	if err := log.write(file); err != nil {
		return fmt.Errorf("Write log file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
	}

	e.logs = append(e.logs[:log.entry.GetIndex()], log)
	return nil
}

func (e *Engine) follower() {
	e.cancelElectionTimeout = make(chan bool)
	e.electionTimer.start()
	select {
	case <-e.electionTimer.timeout():
		e.role = roleCandidate
		e.logger.Info("Follower timeout: convert to candidate | states=%v", e)
	case <-e.cancelElectionTimeout:
		e.electionTimer.stop()
	}
}

func (e *Engine) candidate() {

	if err := e.setCurrentTerm(e.currentTerm + 1); err != nil {
		e.logger.Error("Increase current term failed | err=[%v]", err)
		return
	}

	if err := e.setVotedFor(e.nodeID); err != nil {
		e.logger.Error("Vote for self failed | err=[%v]", err)
		return
	}

	e.electionTimer.start()
	majority := make(chan bool)
	e.requestVotesAsync(majority)

	select {
	case <-e.electionTimer.timeout():
		e.logger.Info("Candidate timeout: start new election | states=%v", e)
	case suc := <-majority:
		if suc {
			e.electionTimer.stop()
			e.role = roleLeader
			e.leaderID = e.nodeID
			e.logger.Info("Candidate received votes from majority: new leader elected | states=%v", e)
		} else {
			// not receive majority, wait timeout before starting new election
			<-e.electionTimer.timeout()
			e.logger.Info("Candidate failed to receive votes from majority: start new election | states=%v", e)
		}
	}
}

func (e *Engine) setCurrentTerm(newTerm uint64) error {

	filePath := path.Join(e.raftdir, fileCurrentTerm)
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("Open currentTerm file failed | path=%v | newTerm=%v | states=%v | err=[%w]",
			filePath, newTerm, e, err)
	}
	defer file.Close()

	if err = binary.Write(file, binary.BigEndian, newTerm); err != nil {
		return fmt.Errorf("Write currentTerm file failed | path=%v | newTerm=%v | states=%v | err=[%w]",
			filePath, newTerm, e, err)
	}

	e.currentTerm = newTerm
	return nil
}

func (e *Engine) setVotedFor(nodeID int32) error {

	filePath := path.Join(e.raftdir, fileVotedFor)
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("Open votedFor file failed | path=%v | nodeID=%v | states=%v | err=[%w]",
			filePath, nodeID, e, err)
	}
	defer file.Close()

	if err = binary.Write(file, binary.BigEndian, nodeID); err != nil {
		return fmt.Errorf("Write votedFor file failed | path=%v | nodeID=%v | states=%v | err=[%w]",
			filePath, nodeID, e, err)
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
			LastLogIndex: lastLog.entry.GetIndex(),
			LastLogTerm:  lastLog.entry.GetTerm(),
		}

		resp, err := e.clients[targetID].RequestVote(context.Background(), req)
		if err != nil {
			e.logger.Error("RequestVote failed | targetID=%v | req=%v | states=%v | err=[%v]", targetID, req, e, err)
			votes <- false
			return
		}
		e.logger.Info("RequestVote sent | targetID=%v | req=%v | resp=%v | states=%v", targetID, req, resp, e)

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

	e.logger.Info("Leader exited | states=%v", e)
}

func (e *Engine) appendEntriesAsync(targetID int32, exits chan bool) {
	go func() {
		for e.role == roleLeader {
			nextIndex := e.nextIndex[targetID]
			prevLog := e.logs[nextIndex-1]
			req := &pb.AppendEntriesReq{
				Term:         e.currentTerm,
				LeaderID:     e.nodeID,
				PrevLogIndex: prevLog.entry.GetIndex(),
				PrevLogTerm:  prevLog.entry.GetTerm(),
				Entries:      make([]*pb.AppendEntriesReq_LogEntry, 0),
				LeaderCommit: e.commitIndex,
			}

			lastLogIndex := uint64(len(e.logs) - 1)
			for ; nextIndex <= lastLogIndex; nextIndex++ {
				req.Entries = append(req.Entries, e.logs[nextIndex].entry)
			}

			resp, err := e.clients[targetID].AppendEntries(context.Background(), req)
			if err != nil {
				e.logger.Error("AppendEntries failed | targetID=%v | req=%v | states=%v | err=[%v]",
					targetID, req, e, err)
				e.waitHeartbeatInterval()
				continue
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
	if logIndex > e.commitIndex && e.logs[logIndex].entry.GetTerm() == e.currentTerm {
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

func (e *Engine) convertToFollower(newTerm uint64) error {
	if err := e.setVotedFor(e.nodeProvider.IDNil()); err != nil {
		return fmt.Errorf("Reset votedFor failed | newTerm=%v | err=[%w]", newTerm, err)
	}
	if err := e.setCurrentTerm(newTerm); err != nil {
		return fmt.Errorf("Set new currentTerm failed | newTerm=%v | err=[%w]", newTerm, err)
	}
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
