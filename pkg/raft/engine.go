package raft

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/chuyangliu/rawkv/pkg/cluster"
	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/pb"
)

// Engine exposes Raft operations.
type Engine struct {
	nodeProvider cluster.NodeProvider
	logger       *logging.Logger
}

// NewEngine instantiates an Engine.
func NewEngine(logLevel int) (*Engine, error) {

	// create node provider
	nodeProvider, err := cluster.NewK8SNodeProvider(logLevel)
	if err != nil {
		return nil, fmt.Errorf("Create Kubernetes node provider failed | err=[%w]", err)
	}

	// instantiate
	return &Engine{
		nodeProvider: nodeProvider,
		logger:       logging.New(logLevel),
	}, nil
}

// Run starts Raft service on current node.
func (e *Engine) Run() {
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
