package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"time"

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

	rpcTimeout int64 = 100 // milliseconds

	electionTimeoutMin int64 = 1500 // milliseconds
	electionTimeoutMax int64 = 3000 // milliseconds
	heartbeatInterval  int64 = 500  // milliseconds

	persistQueueLen int = 1000
)

// ApplyFunc applies a raft log to state machine.
type ApplyFunc func(log *Log) error

// Engine manages raft states and operations.
type Engine struct {
	logger *logging.Logger

	// root directory to store raft states and logs
	raftdir string

	// query cluster size, node ids, etc
	clusterMeta cluster.Meta

	// mutex lock to protect concurrent modifications to states
	// lock sync.Mutex

	// function provided by user to apply a given raft log to state machine
	applyFunc ApplyFunc

	// persistent state on all servers
	currentTerm uint64
	votedFor    int32
	logs        []*Log // log index starts at 1

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

	// heartbeat timer to control empty AppendEntries sent during idle periods
	heartbeatTimer *raftTimer

	// channel to cancel follower's election timeout
	electionTimeoutCanceled chan struct{}

	// queue to implement producer and consumer model for persist requests
	queue *persistQueue
}

// NewEngine instantiates an Engine.
func NewEngine(logLevel int, rootdir string, clusterMeta cluster.Meta) (*Engine, error) {

	// create directory to store raft states
	raftdir := path.Join(rootdir, dirRaft)
	if err := os.MkdirAll(raftdir, 0777); err != nil {
		return nil, fmt.Errorf("Create root directory failed | raftdir=%v | err=[%w]", raftdir, err)
	}

	engine := &Engine{
		logger:      logging.New(logLevel),
		raftdir:     raftdir,
		clusterMeta: clusterMeta,
	}

	if err := engine.init(); err != nil {
		return nil, fmt.Errorf("Initialize raft engine failed | err=[%w]", err)
	}

	return engine, nil
}

func (e *Engine) String() string {
	return fmt.Sprintf("[raftdir=%v | clusterMeta=%v | currentTerm=%v | votedFor=%v | logs=%v"+
		" | commitIndex=%v | lastApplied=%v | nextIndex=%v | matchIndex=%v | role=%v | leaderID=%v]", e.raftdir,
		e.clusterMeta, e.currentTerm, e.votedFor, e.logs, e.commitIndex, e.lastApplied, e.nextIndex,
		e.matchIndex, e.role, e.leaderID)
}

func (e *Engine) init() error {

	// init persistent state: currentTerm
	if err := e.initCurrentTerm(path.Join(e.raftdir, fileCurrentTerm)); err != nil {
		return fmt.Errorf("Initialize currentTerm failed | err=[%w]", err)
	}

	// init persistent state: votedFor
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
	e.nextIndex = make([]uint64, e.clusterMeta.Size())
	e.matchIndex = make([]uint64, e.clusterMeta.Size())
	e.initLeaderStates()

	// init metadata
	e.role = roleFollower
	e.leaderID = e.clusterMeta.NodeIDNil()

	// init timers and timeouts
	e.electionTimer = newRaftTimerRand(e.logger.Level(), electionTimeoutMin, electionTimeoutMax)
	e.heartbeatTimer = newRaftTimer(e.logger.Level(), heartbeatInterval)

	// init channels
	e.electionTimeoutCanceled = make(chan struct{})

	// init persist queue
	e.queue = newPersistQueue(persistQueueLen)

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
			e.votedFor = e.clusterMeta.NodeIDNil()
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
	e.logs = []*Log{{
		entry: &pb.AppendEntriesReq_LogEntry{
			Index: 0,
			Term:  0,
		},
	}}

	for {
		log, err := newLogFromFile(file)
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

func (e *Engine) initLeaderStates() {
	// e.lock.Lock()
	// defer e.lock.Unlock()
	for i := range e.nextIndex {
		e.nextIndex[i] = uint64(len(e.logs))
		e.matchIndex[i] = 0
	}
}

// SetApplyFunc sets raft log apply function to f.
func (e *Engine) SetApplyFunc(f ApplyFunc) {
	e.applyFunc = f
}

// LeaderID returns the current node id of the leader.
func (e *Engine) LeaderID() int32 {
	// e.lock.Lock()
	// defer e.lock.Unlock()
	return e.leaderID
}

// IsLeader returns whether current node is the leader.
func (e *Engine) IsLeader() bool {
	// e.lock.Lock()
	// defer e.lock.Unlock()
	return e.role == roleLeader
}

// RequestVoteHandler handles received RequestVote RPC.
func (e *Engine) RequestVoteHandler(req *pb.RequestVoteReq) *pb.RequestVoteResp {

	resp := &pb.RequestVoteResp{
		Term:        e.currentTerm,
		VoteGranted: false,
	}

	// e.lock.Lock()
	// defer e.lock.Unlock()

	if req.GetTerm() < e.currentTerm {
		e.logger.Info("RequestVote received from older term | req=%v | states=%v", req, e)
		return resp
	}

	if req.GetTerm() > e.currentTerm {
		e.logger.Info("RequestVote received from newer term: convert to follower | req=%v | states=%v", req, e)
		if err := e.convertToFollower(req.GetTerm()); err != nil {
			e.logger.Error("Convert to follower failed | err=[%v]", err)
			return resp
		}
	}

	if (e.votedFor == e.clusterMeta.NodeIDNil() || e.votedFor == req.CandidateID) &&
		e.atLeastUpToDate(req.LastLogIndex, req.LastLogTerm) {
		if err := e.setVotedFor(req.CandidateID); err != nil {
			e.logger.Error("Vote candidate failed | req=%v | err=[%v]", req, err)
			return resp
		}
		resp.VoteGranted = true
		e.cancelElectionTimeout()
	}

	e.logger.Info("RequestVote handled | req=%v | resp=%v | states=%v", req, resp, e)
	return resp
}

func (e *Engine) atLeastUpToDate(lastLogIndex uint64, lastLogTerm uint64) bool {
	lastLog := e.logs[len(e.logs)-1]
	if lastLogTerm == lastLog.entry.GetTerm() {
		return lastLogIndex >= lastLog.entry.GetIndex()
	}
	return lastLogTerm >= lastLog.entry.GetTerm()
}

// AppendEntriesHandler handles received AppendEntries RPC.
func (e *Engine) AppendEntriesHandler(req *pb.AppendEntriesReq) *pb.AppendEntriesResp {

	resp := &pb.AppendEntriesResp{
		Term:    e.currentTerm,
		Success: false,
	}

	// e.lock.Lock()
	// defer e.lock.Unlock()

	if req.GetTerm() < e.currentTerm {
		e.logger.Info("AppendEntries received from older term | req=%v | states=%v", req, e)
		return resp
	}

	if req.GetTerm() > e.currentTerm || e.role == roleCandidate {
		e.logger.Info("AppendEntries received from newer term: convert to follower | req=%v | states=%v", req, e)
		if err := e.convertToFollower(req.GetTerm()); err != nil {
			e.logger.Error("Convert to follower failed | err=[%v]", err)
			return resp
		}
	}

	if e.role == roleLeader {
		panic(fmt.Sprintf("More than one leader exists in the cluster (error most likely caused by incorrect raft"+
			" implementation) | req=%v | states=%v", req, e))
	}

	e.leaderID = req.GetLeaderID()
	e.cancelElectionTimeout()

	prevLogIndex := req.GetPrevLogIndex()
	if prevLogIndex >= uint64(len(e.logs)) || e.logs[prevLogIndex].entry.GetTerm() != req.GetPrevLogTerm() {
		e.logger.Info("AppendEntries logs did not match | req=%v | states=%v", req, e)
		return resp
	}

	index := prevLogIndex + 1
	for _, entry := range req.GetEntries() {
		if entry.GetIndex() != index {
			panic(fmt.Sprintf("Incorrect AppendEntries log index (error most likely caused by incorrect raft"+
				" implementation) | req=%v | states=%v", req, e))
		}
		if index >= uint64(len(e.logs)) {
			if err := e.appendLog(newLogFromPb(entry)); err != nil {
				e.logger.Error("Append log failed | err=[%v]", err)
				return resp
			}
		} else {
			if entry.GetTerm() != e.logs[index].entry.GetTerm() {
				// conflict entry (same index but different terms), delete the existing entry and all that follow it
				if err := e.truncateAndAppendLog(newLogFromPb(entry)); err != nil {
					e.logger.Error("Truncate and append log failed | err=[%v]", err)
					return resp
				}
			}
		}
		index++
	}

	if req.GetLeaderCommit() > e.commitIndex {
		e.commitIndex = min(req.GetLeaderCommit(), uint64(len(e.logs)-1))
		if err := e.apply(); err != nil {
			e.logger.Error("Apply failed | err=[%w]", err)
			return resp
		}
	}

	resp.Success = true
	e.logger.Debug("AppendEntries handled | req=%v | resp=%v | states=%v", req, resp, e)
	return resp
}

func (e *Engine) appendLog(log *Log) error {

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

func (e *Engine) truncateAndAppendLog(log *Log) error {

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

func (e *Engine) cancelElectionTimeout() {
	select {
	case e.electionTimeoutCanceled <- struct{}{}:
	default:
	}
}

// Run starts raft service on current node.
func (e *Engine) Run() error {
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

func (e *Engine) follower() {
	e.electionTimer.start()
	select {
	case <-e.electionTimer.timeout():
		// e.lock.Lock()
		e.role = roleCandidate
		// e.lock.Unlock()
		e.logger.Info("Follower timeout: convert to candidate | states=%v", e)
	case <-e.electionTimeoutCanceled:
		e.electionTimer.stop()
	}
}

func (e *Engine) candidate() {
	// e.lock.Lock()

	if err := e.setCurrentTerm(e.currentTerm + 1); err != nil {
		// e.lock.Unlock()
		e.logger.Error("Increase current term failed | err=[%v]", err)
		return
	}

	if err := e.setVotedFor(e.clusterMeta.NodeIDSelf()); err != nil {
		// e.lock.Unlock()
		e.logger.Error("Vote for self failed | err=[%v]", err)
		return
	}

	// e.lock.Unlock()

	e.electionTimer.start()
	majority := make(chan struct{})
	go e.broadcastRequestVote(majority)

	select {
	case <-e.electionTimer.timeout():
		e.logger.Info("Candidate timeout: start new election | states=%v", e)
	case <-majority:
		e.electionTimer.stop()
		// e.lock.Lock()
		e.role = roleLeader
		e.leaderID = e.clusterMeta.NodeIDSelf()
		// e.lock.Unlock()
		e.logger.Info("Candidate received votes from majority: new leader elected | states=%v", e)
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

func (e *Engine) broadcastRequestVote(majority chan struct{}) {
	votes := make(chan bool)

	for id := int32(0); id < e.clusterMeta.Size(); id++ {
		if id != e.clusterMeta.NodeIDSelf() {
			go e.requestVote(id, votes)
		}
	}

	numGrant := int32(1) // start at 1 since candidate votes for self before requesting votes to others
	for id := int32(0); id < e.clusterMeta.Size(); id++ {
		if id != e.clusterMeta.NodeIDSelf() && <-votes {
			numGrant++
		}
	}

	if e.isMajority(numGrant) {
		majority <- struct{}{}
	}
}

func (e *Engine) requestVote(targetID int32, votes chan bool) {
	// e.lock.Lock()
	lastLog := e.logs[len(e.logs)-1]
	req := &pb.RequestVoteReq{
		Term:         e.currentTerm,
		CandidateID:  e.clusterMeta.NodeIDSelf(),
		LastLogIndex: lastLog.entry.GetIndex(),
		LastLogTerm:  lastLog.entry.GetTerm(),
	}
	// e.lock.Unlock()

	resp, err := e.requestVoteRPC(targetID, req)
	if err != nil {
		e.logger.Error("RequestVote failed | targetID=%v | req=%v | states=%v | err=[%v]",
			targetID, req, e, err)
		votes <- false
		return
	}

	// e.lock.Lock()
	if resp.GetTerm() > e.currentTerm {
		if err := e.convertToFollower(resp.GetTerm()); err != nil {
			e.logger.Error("Convert to follower failed | err=[%v]", err)
		}
	}
	// e.lock.Unlock()

	votes <- resp.VoteGranted
}

func (e *Engine) requestVoteRPC(targetID int32, req *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rpcTimeout)*time.Millisecond)
	defer cancel()
	resp, err := e.clusterMeta.RaftClient(targetID).RequestVote(ctx, req)
	if err != nil {
		return nil, err
	}
	e.logger.Info("RequestVote | targetID=%v | req=%v | resp=%v | states=%v", targetID, req, resp, e)
	return resp, nil
}

// Persist replicates a raft log and applies it to state machine if succeeds.
func (e *Engine) Persist(log *Log) error {
	// e.lock.Lock()

	// set log index and term
	log.entry.Index = uint64(len(e.logs))
	log.entry.Term = e.currentTerm

	// append log to disk
	if err := e.appendLog(log); err != nil {
		// e.lock.Unlock()
		return fmt.Errorf("Append log failed | log=%v | err=[%w]", log, err)
	}

	// e.lock.Unlock()

	// add persist task to queue
	task := newPersistTask(log.entry.Index)
	e.queue.push(task)

	// wait task completion
	if !<-task.done {
		return fmt.Errorf("Persist log task failed | log=%v | err=[%w]", log, task.err)
	}

	return nil
}

func (e *Engine) persistNoOp() {
	if err := e.Persist(newNoOpLog()); err != nil {
		e.logger.Error("Persist no-op log failed | states=%v | err=[%v]", e, err)
	} else {
		e.logger.Info("No-op log persisted | states=%v", e)
	}
}

func (e *Engine) leader() {
	e.initLeaderStates()
	go e.persistNoOp()

	for e.IsLeader() {
		task := (*persistTask)(nil)

		e.heartbeatTimer.start()
		select {
		case <-e.heartbeatTimer.timeout():
		case task, _ = <-e.queue.pop():
			e.heartbeatTimer.stop()
		}

		// e.lock.Lock()
		if task != nil && e.lastApplied >= task.logIndex {
			// e.lock.Unlock()
			task.done <- true
			continue
		}
		// e.lock.Unlock()

		for !e.broadcastAppendEntries() {
			e.logger.Error("AppendEntries failed to succeed on majority (retry after %v ms) | task=%v",
				heartbeatInterval, task)
			time.Sleep(time.Duration(heartbeatInterval))
		}

		// e.lock.Lock()
		// defer e.lock.Unlock()

		if err := e.apply(); err != nil {
			if task != nil {
				task.err = err
				task.done <- false
			}
			e.logger.Error("Apply logs failed | task=%v | err=[%v]", task, err)
			continue
		}

		if task != nil {
			if e.lastApplied < task.logIndex {
				panic(fmt.Sprintf("Expect persist task log applied (error most likely caused by incorrect raft"+
					" implementation) | task=%v | states=%v", task, e))
			}
			task.done <- true
		}
	}

	e.logger.Info("Leader exited | states=%v", e)
}

func (e *Engine) broadcastAppendEntries() bool {
	suc := make(chan bool)

	for id := int32(0); id < e.clusterMeta.Size(); id++ {
		if id != e.clusterMeta.NodeIDSelf() {
			go e.appendEntries(id, suc)
		}
	}

	numSuc := int32(1) // start at 1 since logs already replicated on current node
	for id := int32(0); id < e.clusterMeta.Size(); id++ {
		if id != e.clusterMeta.NodeIDSelf() && <-suc {
			numSuc++
		}
	}

	return e.isMajority(numSuc)
}

func (e *Engine) appendEntries(targetID int32, suc chan bool) {
	// e.lock.Lock()

	nextIndex := e.nextIndex[targetID]
	prevLog := e.logs[nextIndex-1]
	req := &pb.AppendEntriesReq{
		Term:         e.currentTerm,
		LeaderID:     e.clusterMeta.NodeIDSelf(),
		PrevLogIndex: prevLog.entry.GetIndex(),
		PrevLogTerm:  prevLog.entry.GetTerm(),
		Entries:      make([]*pb.AppendEntriesReq_LogEntry, 0),
		LeaderCommit: e.commitIndex,
	}

	lastLogIndex := uint64(len(e.logs) - 1)
	for i := nextIndex; i <= lastLogIndex; i++ {
		req.Entries = append(req.Entries, e.logs[i].entry)
	}

	// e.lock.Unlock()

	resp, err := e.appendEntriesRPC(targetID, req)
	if err != nil {
		e.logger.Error("AppendEntries failed | targetID=%v | req=%v | states=%v | err=[%v]",
			targetID, req, e, err)
		suc <- false
		return
	}

	// e.lock.Lock()
	// defer e.lock.Unlock()

	if resp.GetTerm() > e.currentTerm {
		if err := e.convertToFollower(resp.GetTerm()); err != nil {
			e.logger.Error("Convert to follower failed | err=[%v]", err)
		}
		suc <- resp.GetSuccess()
		return
	}

	if len(req.Entries) > 0 {
		if resp.GetSuccess() {
			newNextIndex := nextIndex + uint64(len(req.Entries))
			e.nextIndex[targetID] = newNextIndex
			e.matchIndex[targetID] = newNextIndex - 1
			e.commit(nextIndex, newNextIndex)
		} else {
			e.nextIndex[targetID] = max(1, e.nextIndex[targetID]-1)
		}
	}

	suc <- resp.GetSuccess()
}

func (e *Engine) appendEntriesRPC(targetID int32, req *pb.AppendEntriesReq) (*pb.AppendEntriesResp, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rpcTimeout)*time.Millisecond)
	defer cancel()
	resp, err := e.clusterMeta.RaftClient(targetID).AppendEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	e.logger.Info("AppendEntries | targetID=%v | req=%v | resp=%v | states=%v", targetID, req, resp, e)
	return resp, nil
}

func (e *Engine) commit(begIndex uint64, endIndex uint64) {
	for i := begIndex; i < endIndex; i++ {
		if i > e.commitIndex && e.logs[i].entry.GetTerm() == e.currentTerm {
			numMatch := int32(1) // log[i] already replicated on current node
			for id := int32(0); id < e.clusterMeta.Size(); id++ {
				if id != e.clusterMeta.NodeIDSelf() && e.matchIndex[id] >= i {
					numMatch++
				}
			}
			if e.isMajority(numMatch) {
				e.commitIndex = i
			} else {
				break
			}
		}
	}
}

func (e *Engine) apply() error {
	for ; e.commitIndex > e.lastApplied; e.lastApplied++ {
		log := e.logs[e.lastApplied+1]
		if err := e.applyFunc(log); err != nil {
			return fmt.Errorf("Apply log failed | log=%v | states=%v | err=[%w]", log, e, err)
		}
	}
	return nil
}

func (e *Engine) convertToFollower(newTerm uint64) error {
	if err := e.setVotedFor(e.clusterMeta.NodeIDNil()); err != nil {
		return fmt.Errorf("Reset votedFor failed | newTerm=%v | err=[%w]", newTerm, err)
	}
	if err := e.setCurrentTerm(newTerm); err != nil {
		return fmt.Errorf("Set new currentTerm failed | newTerm=%v | err=[%w]", newTerm, err)
	}
	e.role = roleFollower
	return nil
}

func (e *Engine) isMajority(num int32) bool {
	return num > e.clusterMeta.Size()/2
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
