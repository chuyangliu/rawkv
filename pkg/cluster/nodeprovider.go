package cluster

// NodeProvider provides information about nodes in the cluster.
type NodeProvider interface {
	// Index returns the index of current node.
	Index() (int, error)
	// RaftAddr returns the network address of node (with the given index) providing Raft service.
	RaftAddr(index int) (string, error)
	// Size returns the number of nodes in the cluster.
	Size() (int, error)
}
