package raft

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"time"
	"unsafe"

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

	roleLeader    uint8 = 0
	roleFollower  uint8 = 1
	roleCandidate uint8 = 2
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
	e.logger.Info("Raft service started | states=%v", e)

	for {
		time.Sleep(time.Duration(3) * time.Second)

		idx, err := e.nodeProvider.Index()
		if err != nil || idx > 0 {
			if err != nil {
				e.logger.Error("Get current node index failed | err=[%v]", err)
			}
			continue
		}

		size, err := e.nodeProvider.Size()
		if err != nil {
			e.logger.Error("Get number of nodes failed | idx=%v | err=[%v]", idx, err)
			continue
		}

		for i := 0; i < size; i++ {
			if i == idx {
				continue
			}

			addr, err := e.nodeProvider.RaftAddr(i)
			if err != nil {
				e.logger.Error("Get Raft server address failed | idx=%v | size=%v | err=[%v]", idx, size, err)
				continue
			}

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				e.logger.Error("Connect Raft server failed | idx=%v | size=%v | addr=%v | err=[%v]",
					idx, size, addr, err)
				continue
			}
			defer conn.Close()
			client := pb.NewRaftClient(conn)

			req := &pb.AppendEntriesReq{}
			if _, err := client.AppendEntries(context.Background(), req); err != nil {
				e.logger.Error("Send Raft request failed | idx=%v | size=%v | addr=%v | err=[%v]",
					idx, size, addr, err)
				continue
			}
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

	buf := make([]byte, unsafe.Sizeof(e.currentTerm))

	if _, err := io.ReadFull(file, buf); err == nil {
		for i, b := range buf {
			e.currentTerm |= uint64(b) << (i * 8)
		}
	} else if err == io.EOF {
		if _, err = file.Write(buf); err != nil {
			return fmt.Errorf("Write initial currentTerm file failed | path=%v | err=[%w]", filePath, err)
		}
		e.currentTerm = 0
	} else {
		return fmt.Errorf("Read currentTerm file failed | path=%v | err=[%w]", filePath, err)
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
