// Package raft implements raft consensus algorithm.
package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/chuyangliu/rawkv/pkg/algods/algo"
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

	electionTimeoutMin int64 = 1500 // Milliseconds.
	electionTimeoutMax int64 = 3000 // Milliseconds.
	heartbeatInterval  int64 = 500  // Milliseconds.
	rpcTimeout         int64 = 100  // Milliseconds.

	persistQueueLen int = 1000
)

// ApplyFunc applies a raft log to state machine.
type ApplyFunc func(log *Log) error

// Engine implements raft consensus algorithm.
type Engine struct {
	logger *logging.Logger

	// Root directory to store raft states and logs.
	raftdir string

	// Query cluster size, node ids, etc.
	clusterMeta cluster.Meta

	// Mutex lock to protect concurrent accesses.
	lock sync.Mutex

	// Function supplied by user to apply a given raft log to state machine.
	applyFunc ApplyFunc

	// Persistent state on all servers.
	currentTerm uint64
	votedFor    int32
	logs        []*Log // Log index starts at 1.

	// Volatile state on all servers.
	commitIndex uint64
	lastApplied uint64

	// Volatile state on all leaders.
	nextIndex  []uint64
	matchIndex []uint64

	// Queue for leader to handle persistent tasks.
	queue *persistQueue

	// Leader or follower or candidate.
	role uint8

	// Leader node id.
	leaderID int32

	// Election timer.
	electionTimer *raftTimer

	// Heartbeat timer to control empty AppendEntries sent during idle periods.
	heartbeatTimer *raftTimer

	// Channel to cancel follower's election timeout.
	electionTimeoutCanceled chan struct{}
}

// NewEngine creates an Engine with given logging level, root directory, and cluster meta.
func NewEngine(level int, rootdir string, clusterMeta cluster.Meta) (*Engine, error) {

	raftdir := path.Join(rootdir, dirRaft)
	if err := os.MkdirAll(raftdir, 0777); err != nil {
		return nil, fmt.Errorf("Create root directory failed | raftdir=%v | err=[%w]", raftdir, err)
	}

	engine := &Engine{
		logger:      logging.New(level),
		raftdir:     raftdir,
		clusterMeta: clusterMeta,
	}

	if err := engine.init(); err != nil {
		return nil, fmt.Errorf("Initialize raft engine failed | err=[%w]", err)
	}

	return engine, nil
}

// String returns a string representation of current raft states.
func (e *Engine) String() string {
	return fmt.Sprintf("[raftdir=%v | clusterMeta=%v | currentTerm=%v | votedFor=%v | logs=%v"+
		" | commitIndex=%v | lastApplied=%v | nextIndex=%v | matchIndex=%v | role=%v | leaderID=%v]", e.raftdir,
		e.clusterMeta, e.currentTerm, e.votedFor, e.logs, e.commitIndex, e.lastApplied, e.nextIndex,
		e.matchIndex, e.role, e.leaderID)
}

// init initializes raft states.
func (e *Engine) init() error {

	// Init persistent state: currentTerm.
	if err := e.initCurrentTerm(path.Join(e.raftdir, fileCurrentTerm)); err != nil {
		return fmt.Errorf("Initialize currentTerm failed | err=[%w]", err)
	}

	// Init persistent state: votedFor.
	if err := e.initVotedFor(path.Join(e.raftdir, fileVotedFor)); err != nil {
		return fmt.Errorf("Initialize votedFor failed | err=[%w]", err)
	}

	// Init persistent state: logs.
	if err := e.initLogs(path.Join(e.raftdir, fileLogs)); err != nil {
		return fmt.Errorf("Initialize logs failed | err=[%w]", err)
	}

	// Init volatile states on all servers.
	e.commitIndex = 0
	e.lastApplied = 0

	// Init volatile states on leaders.
	e.nextIndex = make([]uint64, e.clusterMeta.Size())
	e.matchIndex = make([]uint64, e.clusterMeta.Size())
	e.initLeaderStates()

	// Init metadata.
	e.role = roleFollower
	e.leaderID = e.clusterMeta.NodeIDNil()

	// Init timers and timeouts.
	e.electionTimer = newRaftTimerRand(electionTimeoutMin, electionTimeoutMax)
	e.heartbeatTimer = newRaftTimer(heartbeatInterval)

	// Init channels.
	e.electionTimeoutCanceled = make(chan struct{})

	return nil
}

// initCurrentTerm initializes persistent state: currentTerm.
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

// initVotedFor initializes persistent states: votedFor.
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

// initLogs initializes persistent state: logs.
func (e *Engine) initLogs(filePath string) error {

	file, err := os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("Open logs file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
	}
	defer file.Close()

	// Add a dummy log to make log index start at 1.
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
		if log == nil { // EOF.
			break
		}
		e.logs = append(e.logs, log)
	}

	return nil
}

// initLeaderStates initializes leader-only states.
func (e *Engine) initLeaderStates() {
	e.lock.Lock()
	defer e.lock.Unlock()
	for i := range e.nextIndex {
		e.nextIndex[i] = uint64(len(e.logs))
		e.matchIndex[i] = 0
	}
	e.queue = newPersistQueue(persistQueueLen)
}

// SetApplyFunc sets raft log apply function to f.
func (e *Engine) SetApplyFunc(f ApplyFunc) {
	e.applyFunc = f
}

// LeaderID returns the current node id of the leader.
func (e *Engine) LeaderID() int32 {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.leaderID
}

// IsLeader returns whether current node is the leader.
func (e *Engine) IsLeader() bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.role == roleLeader
}

// RequestVoteHandler handles received RequestVote RPC.
func (e *Engine) RequestVoteHandler(ctx context.Context, req *pb.RequestVoteReq) *pb.RequestVoteResp {

	resp := &pb.RequestVoteResp{
		Term:        e.currentTerm,
		VoteGranted: false,
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	if ctx.Err() == context.Canceled {
		e.logger.Warn("RequestVote canceled by client | states=%v", e)
		return nil
	}

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

// atLeastUpToDate returns whether a candidate log with given last log
// index/term is at least up to date as current node's log.
func (e *Engine) atLeastUpToDate(lastLogIndex uint64, lastLogTerm uint64) bool {
	lastLog := e.logs[len(e.logs)-1]
	if lastLogTerm == lastLog.entry.GetTerm() {
		return lastLogIndex >= lastLog.entry.GetIndex()
	}
	return lastLogTerm >= lastLog.entry.GetTerm()
}

// AppendEntriesHandler handles received AppendEntries RPC.
func (e *Engine) AppendEntriesHandler(ctx context.Context, req *pb.AppendEntriesReq) *pb.AppendEntriesResp {

	resp := &pb.AppendEntriesResp{
		Term:    e.currentTerm,
		Success: false,
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	if ctx.Err() == context.Canceled {
		e.logger.Warn("AppendEntries canceled by client | states=%v", e)
		return nil
	}

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
				// Conflicting entries (same index but different terms):
				// Delete the existing entry and all that follow it.
				if err := e.truncAppendLog(newLogFromPb(entry)); err != nil {
					e.logger.Error("Truncate and append log failed | err=[%v]", err)
					return resp
				}
			}
		}
		index++
	}

	if req.GetLeaderCommit() > e.commitIndex {
		e.commitIndex = algo.Min(req.GetLeaderCommit(), uint64(len(e.logs)-1))
		if err := e.apply(); err != nil {
			e.logger.Error("Apply failed | err=[%w]", err)
			return resp
		}
	}

	resp.Success = true
	e.logger.Debug("AppendEntries handled | req=%v | resp=%v | states=%v", req, resp, e)
	return resp
}

// appendLog appends a raft log to logs on disk.
func (e *Engine) appendLog(log *Log) error {

	filePath := path.Join(e.raftdir, fileLogs)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("Open logs file failed | log=%v | path=%v | states=%v | err=[%w]", log, filePath, e, err)
	}
	defer file.Close()

	// Append.
	if err := log.write(file); err != nil {
		return fmt.Errorf("Write log file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
	}

	// Update in-memory logs.
	e.logs = append(e.logs, log)

	return nil
}

// truncAppendLog deletes all logs on disk starting at given log's index and append the given log.
func (e *Engine) truncAppendLog(log *Log) error {

	filePath := path.Join(e.raftdir, fileLogs)
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("Open logs file failed | log=%v | path=%v | states=%v | err=[%w]", log, filePath, e, err)
	}
	defer file.Close()

	// Calculate file size to retain after truncate.
	size := int64(0)
	for i := uint64(1); i < log.entry.GetIndex(); i++ {
		size += e.logs[i].size()
	}

	// Truncate.
	if err := file.Truncate(size); err != nil {
		return fmt.Errorf("Truncate log file failed | log=%v | path=%v | states=%v | err=[%w]", log, filePath, e, err)
	}

	// Seek to file end.
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("Seek log file failed | log=%v | path=%v | states=%v | err=[%w]", log, filePath, e, err)
	}

	// Append.
	if err := log.write(file); err != nil {
		return fmt.Errorf("Write log file failed | path=%v | states=%v | err=[%w]", filePath, e, err)
	}

	// Update in-memory logs.
	e.logs = append(e.logs[:log.entry.GetIndex()], log)

	return nil
}

// cancelElectionTimeout cancels running election timer.
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

// follower executes raft follower's rules.
func (e *Engine) follower() {
	e.electionTimer.start()
	select {
	case <-e.electionTimer.timeout():
		e.lock.Lock()
		e.role = roleCandidate
		e.lock.Unlock()
		e.logger.Info("Follower timeout: convert to candidate | states=%v", e)
	case <-e.electionTimeoutCanceled:
		e.electionTimer.stop()
	}
}

// candidate executes raft candidate's rules.
func (e *Engine) candidate() {
	e.lock.Lock()
	defer e.lock.Unlock()

	if err := e.setCurrentTerm(e.currentTerm + 1); err != nil {
		e.logger.Error("Increase current term failed | err=[%v]", err)
		return
	}

	if err := e.setVotedFor(e.clusterMeta.NodeIDSelf()); err != nil {
		e.logger.Error("Vote for self failed | err=[%v]", err)
		return
	}

	e.electionTimer.start()
	majority := make(chan struct{})
	go e.broadcastRequestVote(majority)

	select {
	case <-e.electionTimer.timeout():
		e.logger.Info("Candidate timeout: start new election | states=%v", e)
	case <-majority:
		e.electionTimer.stop()
		e.role = roleLeader
		e.leaderID = e.clusterMeta.NodeIDSelf()
		e.logger.Info("Candidate received votes from majority: new leader elected | states=%v", e)
	}
}

// setCurrentTerm sets currentTerm to newTerm.
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

// setVotedFor sets votedFor to nodeID.
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

// broadcastRequestVote sends RequestVote RPCs to each other nodes in parallel.
// If votes granted by majority of nodes, a dummy value will be sent to majority channel.
func (e *Engine) broadcastRequestVote(majority chan<- struct{}) {

	req := e.buildRequestVoteRequest()
	votes := make(chan bool)
	mutex := &sync.Mutex{}

	for id := int32(0); id < e.clusterMeta.Size(); id++ {
		if id != e.clusterMeta.NodeIDSelf() {
			go e.requestVote(id, req, votes, mutex)
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

// buildRequestVoteRequest builds a RequestVote RPC request.
func (e *Engine) buildRequestVoteRequest() *pb.RequestVoteReq {
	lastLog := e.logs[len(e.logs)-1]
	return &pb.RequestVoteReq{
		Term:         e.currentTerm,
		CandidateID:  e.clusterMeta.NodeIDSelf(),
		LastLogIndex: lastLog.entry.GetIndex(),
		LastLogTerm:  lastLog.entry.GetTerm(),
	}
}

// requestVote sends a RequestVote RPC request req to the node with id targetID.
// If the target node grants the vote, a true value will be sent to votes channel.
// Otherwise, a false value will be sent. The mutex is used to protect concurrent accessess.
func (e *Engine) requestVote(targetID int32, req *pb.RequestVoteReq, votes chan<- bool, mutex *sync.Mutex) {

	resp, err := e.requestVoteRPC(targetID, req)
	if err != nil {
		e.logger.Error("RequestVote failed | targetID=%v | req=%v | states=%v | err=[%v]",
			targetID, req, e, err)
		votes <- false
		return
	}

	mutex.Lock()
	if resp.GetTerm() > e.currentTerm {
		if err := e.convertToFollower(resp.GetTerm()); err != nil {
			e.logger.Error("Convert to follower failed | err=[%v]", err)
		}
	}
	mutex.Unlock()

	votes <- resp.VoteGranted
}

// requestVoteRPC sends a RequestVote RPC request req to the node with id targetID.
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
func (e *Engine) Persist(log *Log) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// Handle panic when sending on a closed queue.
			err = fmt.Errorf("Persist failed: current node is not the leader | err=[%v]", r)
		}
	}()

	e.lock.Lock()

	// Check if current node is the leader.
	if e.role != roleLeader {
		e.lock.Unlock()
		return fmt.Errorf("Persist failed: current node is not the leader | states=%v", e)
	}

	// Set log index and term.
	log.entry.Index = uint64(len(e.logs))
	log.entry.Term = e.currentTerm

	// Append log to disk.
	if err := e.appendLog(log); err != nil {
		e.lock.Unlock()
		return fmt.Errorf("Append log failed | log=%v | err=[%w]", log, err)
	}

	e.lock.Unlock()

	// Push persist task to queue.
	task := newPersistTask(log.entry.Index)
	e.queue.push(task)

	// Wait task completion.
	if !<-task.success {
		return fmt.Errorf("Persist log task failed | log=%v | err=[%w]", log, task.err)
	}

	return nil
}

// persistNoOp persists a no-op raft log on disk.
func (e *Engine) persistNoOp() {
	if err := e.Persist(newNoOpLog()); err != nil {
		e.logger.Error("Persist no-op log failed | states=%v | err=[%v]", e, err)
	} else {
		e.logger.Info("No-op log persisted | states=%v", e)
	}
}

// leader executes raft leader's rules.
func (e *Engine) leader() {

	// Reset leader states.
	e.initLeaderStates()

	// Persist a no-op log.
	go e.persistNoOp()

	for {
		task := (*persistTask)(nil)

		// Wait heartbeat or persist task.
		e.heartbeatTimer.start()
		select {
		case <-e.heartbeatTimer.timeout():
		case task = <-e.queue.pop():
			e.heartbeatTimer.stop()
		}

		e.lock.Lock()

		// Check if current node is still the leader.
		if e.role != roleLeader {
			e.drainQueue()
			e.lock.Unlock()
			if task != nil {
				task.err = fmt.Errorf("Current node is not the leader | states=%v", e)
				task.success <- false
			}
			break
		}

		// Check if task has been completed.
		if task != nil && task.index <= e.lastApplied {
			e.lock.Unlock()
			task.success <- true
			continue
		}

		// Replicate logs to other nodes.
		if !e.broadcastAppendEntries() {
			e.lock.Unlock()
			go e.queue.push(task)
			e.logger.Error("AppendEntries failed to succeed on majority: push task back to queue | task=%v", task)
			continue
		}

		// Apply logs to state machine.
		if err := e.apply(); err != nil {
			e.lock.Unlock()
			if task != nil {
				task.err = err
				task.success <- false
			}
			e.logger.Error("Apply logs failed | task=%v | err=[%v]", task, err)
			continue
		}

		if task == nil {
			e.lock.Unlock()
		} else {
			if task.index > e.lastApplied {
				panic(fmt.Sprintf("Expect persist task log applied (error most likely caused by incorrect raft"+
					" implementation) | task=%v | states=%v", task, e))
			}
			e.lock.Unlock()
			task.success <- true
		}
	}

	e.logger.Info("Leader exited | states=%v", e)
}

// drainQueue closes persist queue and marks all remaining tasks in the queue as failed.
// This method must be called only when current node is no longer the leader.
func (e *Engine) drainQueue() {
	e.queue.close()
	for task := range e.queue.queue {
		task.err = fmt.Errorf("Drained task from queue: current node is not the leader | states=%v", e)
		task.success <- false
	}
}

// broadcastAppendEntries sends AppendEntries RPCs to each other nodes in parallel.
// Return true if majority of nodes successfully complete the request, false otherwise.
func (e *Engine) broadcastAppendEntries() bool {
	success := make(chan bool)
	mutex := &sync.Mutex{}

	for id := int32(0); id < e.clusterMeta.Size(); id++ {
		if id != e.clusterMeta.NodeIDSelf() {
			req := e.buildAppendEntriesRequest(id)
			go e.appendEntries(id, req, success, mutex)
		}
	}

	numSuccess := int32(1) // start at 1 since logs already replicated on current node
	for id := int32(0); id < e.clusterMeta.Size(); id++ {
		if id != e.clusterMeta.NodeIDSelf() && <-success {
			numSuccess++
		}
	}

	return e.isMajority(numSuccess)
}

// buildAppendEntriesRequest builds an AppendEntries RPC request to node with given targetID.
func (e *Engine) buildAppendEntriesRequest(targetID int32) *pb.AppendEntriesReq {

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

	return req
}

// appendEntries sends AppendEntries RPC request req to the node with id targetID.
// If the target node successfully completes the request, a true value will be sent to success channel.
// Otherwise, a false value will be sent. The mutex is used to protect concurrent accessess.
func (e *Engine) appendEntries(targetID int32, req *pb.AppendEntriesReq, success chan<- bool, mutex *sync.Mutex) {

	resp, err := e.appendEntriesRPC(targetID, req)
	if err != nil {
		e.logger.Error("AppendEntries failed | targetID=%v | req=%v | states=%v | err=[%v]",
			targetID, req, e, err)
		success <- false
		return
	}

	mutex.Lock()

	if e.role != roleLeader {
		mutex.Unlock()
		success <- false
		return
	}

	if resp.GetTerm() > e.currentTerm {
		if err := e.convertToFollower(resp.GetTerm()); err != nil {
			e.logger.Error("Convert to follower failed | err=[%v]", err)
		}
		mutex.Unlock()
		success <- resp.GetSuccess()
		return
	}

	mutex.Unlock()

	if len(req.Entries) > 0 {
		if resp.GetSuccess() {
			nextIndex := e.nextIndex[targetID]
			newNextIndex := nextIndex + uint64(len(req.Entries))
			e.nextIndex[targetID] = newNextIndex
			e.matchIndex[targetID] = newNextIndex - 1
			mutex.Lock()
			e.commit(nextIndex, newNextIndex)
			mutex.Unlock()
		} else {
			e.nextIndex[targetID] = algo.Max(1, e.nextIndex[targetID]-1)
		}
	}

	success <- resp.GetSuccess()
}

// appendEntriesRPC sends AppendEntries RPC request req to the node with id targetID.
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

// commit scans raft logs from begIndex to endIndex (not included) and updates commitIndex if possible.
func (e *Engine) commit(begIndex uint64, endIndex uint64) {
	for i := begIndex; i < endIndex; i++ {
		if i > e.commitIndex && e.logs[i].entry.GetTerm() == e.currentTerm {
			numMatch := int32(1) // log[i] already replicated on current node.
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

// apply invokes user-supplied function to apply raft logs.
func (e *Engine) apply() error {
	for ; e.commitIndex > e.lastApplied; e.lastApplied++ {
		log := e.logs[e.lastApplied+1]
		if err := e.applyFunc(log); err != nil {
			return fmt.Errorf("Apply log failed | log=%v | states=%v | err=[%w]", log, e, err)
		}
	}
	return nil
}

// convertToFollower sets current role to follower and updates states.
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

// isMajority returns whether a given number num is a majority of cluster size.
func (e *Engine) isMajority(num int32) bool {
	return num > e.clusterMeta.Size()/2
}
