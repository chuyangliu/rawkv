package raft

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/chuyangliu/rawkv/pkg/cluster"
	"github.com/chuyangliu/rawkv/pkg/logging"
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

// Engine manages Raft states and operations.
type Engine struct {
	raftdir      string
	nodeProvider cluster.NodeProvider
	logger       *logging.Logger

	// persistent state on all servers
	currentTerm uint64
	votedFor    string
	logs        []*raftLog // log index starts at 1

	// volatile state on all servers
	commitIndex uint64
	lastApplied uint64

	// volatile state on all leaders
	nextIndex  []uint64
	matchIndex []uint64

	// leader or follower or candidate
	role uint8

	// network address of the leader node
	leaderAddr string

	// election timer
	electionTimer     *time.Timer
	electionTimeout   time.Duration
	electionTimeoutCh chan bool
}

// NewEngine instantiates an Engine.
func NewEngine(rootdir string, logLevel int) (*Engine, error) {

	// create node provider
	nodeProvider, err := cluster.NewK8SNodeProvider(logLevel)
	if err != nil {
		return nil, fmt.Errorf("Create Kubernetes node provider failed | rootdir=%v | err=[%w]", rootdir, err)
	}

	// get number of Raft nodes in the cluster
	numNodes, err := nodeProvider.Size()
	if err != nil {
		return nil, fmt.Errorf("Query cluster size failed | rootdir=%v | err=[%w]", rootdir, err)
	}

	// create directory to store Raft states
	raftdir := path.Join(rootdir, dirRaft)
	if err := os.MkdirAll(raftdir, 0777); err != nil {
		return nil, fmt.Errorf("Create root directory failed | raftdir=%v | err=[%w]", raftdir, err)
	}

	// instantiate
	engine := &Engine{
		raftdir:      raftdir,
		nodeProvider: nodeProvider,
		logger:       logging.New(logLevel),

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]uint64, numNodes),
		matchIndex: make([]uint64, numNodes),

		role:       roleFollower,
		leaderAddr: "",

		electionTimer:     nil,
		electionTimeout:   time.Duration(500) * time.Millisecond,
		electionTimeoutCh: make(chan bool),
	}

	// nextIndex starts at 1
	for i := range engine.nextIndex {
		engine.nextIndex[i] = 1
	}

	// init Raft persistent states
	if err := engine.initPersistStates(); err != nil {
		return nil, fmt.Errorf("Initialize Raft persistent states failed | raftdir=%v | err=[%w]", raftdir, err)
	}

	return engine, nil
}

func (e *Engine) String() string {
	return fmt.Sprintf("[raftdir=%v | currentTerm=%v | votedFor=%v | logSize=%v | commitIndex=%v | lastApplied=%v"+
		" | nextIndex=%v | matchIndex=%v | role=%v | leaderAddr=%v]", e.raftdir, e.currentTerm, e.votedFor,
		len(e.logs)-1, e.commitIndex, e.lastApplied, e.nextIndex, e.matchIndex, e.role, e.leaderAddr)
}

// Run starts Raft service on current node.
func (e *Engine) Run() {
	for {
		e.logger.Debug("Print Raft states | states=%v", e)
		switch e.role {
		case roleFollower:
			e.follower()
		case roleCandidate:
			e.candidate()
		case roleLeader:
			e.leader()
		default:
			e.logger.Error("Invalid Raft role | role=%v", e.role)
			break
		}
	}
}

func (e *Engine) initPersistStates() error {

	// init currentTerm
	filePath := path.Join(e.raftdir, fileCurrentTerm)
	if err := e.initCurrentTerm(filePath); err != nil {
		return fmt.Errorf("Initialize currentTerm failed | path=%v | err=[%w]", filePath, err)
	}

	// init votedFor
	filePath = path.Join(e.raftdir, fileVotedFor)
	if err := e.initVotedFor(filePath); err != nil {
		return fmt.Errorf("Initialize votedFor failed | path=%v | err=[%w]", filePath, err)
	}

	// init logs
	filePath = path.Join(e.raftdir, fileLogs)
	if err := e.initLogs(filePath); err != nil {
		return fmt.Errorf("Initialize logs failed | path=%v | err=[%w]", filePath, err)
	}

	return nil
}

func (e *Engine) initCurrentTerm(filePath string) error {

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("Open currentTerm file failed | path=%v | err=[%w]", filePath, err)
	}
	defer file.Close()

	if err := binary.Read(file, binary.BigEndian, &e.currentTerm); err != nil {
		if err == io.EOF {
			e.currentTerm = 0
			if err = binary.Write(file, binary.BigEndian, e.currentTerm); err != nil {
				return fmt.Errorf("Write initial currentTerm file failed | path=%v | err=[%w]", filePath, err)
			}
		} else {
			return fmt.Errorf("Read currentTerm file failed | path=%v | err=[%w]", filePath, err)
		}
	}

	return nil
}

func (e *Engine) initVotedFor(filePath string) error {

	file, err := os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("Open votedFor file failed | path=%v | err=[%w]", filePath, err)
	}
	defer file.Close()

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("Read votedFor file failed | path=%v | err=[%w]", filePath, err)
	}

	e.votedFor = string(bytes)
	return nil
}

func (e *Engine) initLogs(filePath string) error {

	file, err := os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("Open logs file failed | path=%v | err=[%w]", filePath, err)
	}
	defer file.Close()

	e.logs = make([]*raftLog, 1) // log index starts at 1

	for {
		log, err := readLog(file)
		if err != nil {
			return fmt.Errorf("Read log failed | path=%v | err=[%w]", filePath, err)
		}
		if log == nil {
			break
		}
		e.logs = append(e.logs, log)
	}

	return nil
}

func (e *Engine) follower() {
	e.electionTimer = time.AfterFunc(e.electionTimeout, e.electionTimeoutHandler)
	<-e.electionTimeoutCh
}

func (e *Engine) candidate() {

	// increate current term
	if err := e.incrCurrentTerm(); err != nil {
		e.logger.Error("Increase current term failed | err=[%v]", err)
		return
	}

	// vote for self
	if err := e.vote(""); err != nil {
		e.logger.Error("Vote for self failed | err=[%v]", err)
		return
	}

	// start election timer
	e.electionTimer = time.AfterFunc(e.electionTimeout, e.electionTimeoutHandler)

	// send RequestVote RPCs to all other servers
	majority := make(chan bool)
	e.requestVotes(majority)

	// wait timeout or vote replies
	select {
	case <-e.electionTimeoutCh:
		return
	case promote := <-majority:
		if promote {
			e.leaderAddr = e.votedFor
			e.role = roleLeader
		}
	}
}

func (e *Engine) electionTimeoutHandler() {
	if e.role == roleFollower {
		e.role = roleCandidate
	}
	e.electionTimeoutCh <- true
}

func (e *Engine) incrCurrentTerm() error {

	filePath := path.Join(e.raftdir, fileCurrentTerm)
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("Open currentTerm file failed | path=%v | err=[%w]", filePath, err)
	}
	defer file.Close()

	e.currentTerm++
	if err = binary.Write(file, binary.BigEndian, e.currentTerm); err != nil {
		return fmt.Errorf("Write currentTerm file failed | path=%v | err=[%w]", filePath, err)
	}

	return nil
}

func (e *Engine) vote(addr string) error {

	if addr == "" { // vote for self
		idx, err := e.nodeProvider.Index()
		if err != nil {
			return fmt.Errorf("Get current node index failed | err=[%w]", err)
		}
		addr, err = e.nodeProvider.RaftAddr(idx)
		if err != nil {
			return fmt.Errorf("Get current node address failed | nodeIdx=%v | err=[%w]", idx, err)
		}
	}

	filePath := path.Join(e.raftdir, fileVotedFor)
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("Open votedFor file failed | path=%v | err=[%w]", filePath, err)
	}
	defer file.Close()

	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("Truncate votedFor file failed | path=%v | err=[%w]", filePath, err)
	}

	if _, err := file.WriteString(addr); err != nil {
		return fmt.Errorf("Write votedFor file failed | path=%v | err=[%w]", filePath, err)
	}

	return nil
}

func (e *Engine) requestVotes(majority chan bool) {
	// TODO
}

func (e *Engine) leader() {
	// TODO
}
