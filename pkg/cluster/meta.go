package cluster

import (
	"github.com/chuyangliu/rawkv/pkg/pb"
)

// Meta provides information of nodes in the cluster.
type Meta interface {
	// NodeIDNil returns the nil value of node id.
	NodeIDNil() int32
	// NodeIDSelf returns the id of current node.
	NodeIDSelf() int32
	// Size returns the number of nodes in the cluster.
	Size() int32
	// RaftClient returns the grpc raft client to communicate with the node with given id.
	RaftClient(id int32) pb.RaftClient
}