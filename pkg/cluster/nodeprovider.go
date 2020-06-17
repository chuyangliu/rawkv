package cluster

// NodeProvider provides information about nodes in the cluster.
type NodeProvider interface {
	// ID returns the id of current node.
	ID() (int32, error)
	// RaftAddr returns the network address of node (with the given id) providing Raft service.
	RaftAddr(id int32) (string, error)
	// Size returns the number of nodes in the cluster.
	Size() (int32, error)
}
