package cluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/chuyangliu/rawkv/pkg/logging"
)

// Environment variables.
const (
	envPodName     = "RAWKV_POD_NAME"
	envServiceName = "RAWKV_SERVICE_NAME"
	envNamespace   = "RAWKV_NAMESPACE"
	envRaftPort    = "RAWKV_RAFT_PORT"
)

// K8SNodeProvider implements NodeProvider for a Kubernetes cluster.
// An example of cluster config can be found at kubernetes/rawkv.yaml.
type K8SNodeProvider struct {
	idx      int    // current node index
	addrFmt  string // network address format for nodes
	raftPort string // port number for raft service
	logger   *logging.Logger
}

// NewK8SNodeProvider instantiates a new K8SNodeProvider.
func NewK8SNodeProvider(logLevel int) (*K8SNodeProvider, error) {
	logger := logging.New(logLevel)

	// read pod name env
	podName := os.Getenv(envPodName)
	if len(podName) == 0 {
		return nil, fmt.Errorf("Invalid env | env=%v | val=%v", envPodName, podName)
	}

	// parse node index from pod name
	pos := strings.LastIndex(podName, "-")
	if pos < 0 || pos == len(podName) {
		return nil, fmt.Errorf("Invalid pod name | podName=%v", podName)
	}
	idxStr := podName[pos+1:]
	idx, err := strconv.Atoi(idxStr)
	if err != nil || idx < 0 {
		return nil, fmt.Errorf("Invalid node index | podName=%v | err=[%w]", podName, err)
	}

	// read service name env
	serviceName := os.Getenv(envServiceName)
	if len(serviceName) == 0 {
		return nil, fmt.Errorf("Invalid env | env=%v | val=%v", envServiceName, serviceName)
	}

	// read namespace env
	namespace := os.Getenv(envNamespace)
	if len(namespace) == 0 {
		return nil, fmt.Errorf("Invalid env | env=%v | val=%v", envNamespace, namespace)
	}

	// get cluster domain
	clusterDomain, err := getClusterDomain()
	if err != nil {
		return nil, fmt.Errorf("Get cluster domain failed | err=[%w]", err)
	}

	// build address format
	addrFmt := fmt.Sprintf("%s.%s.%s.%s.%s:%s",
		podName[:pos+1]+"%v", serviceName, namespace, "svc", clusterDomain, "%v")

	// read raft port env
	raftPort := os.Getenv(envRaftPort)
	if len(raftPort) == 0 {
		return nil, fmt.Errorf("Invalid env | env=%v | val=%v", envRaftPort, raftPort)
	}

	logger.Info("Kubernetes node provider created | idx=%v | addrFmt=%v | raftPort=%v", idx, addrFmt, raftPort)

	return &K8SNodeProvider{
		idx:      idx,
		addrFmt:  addrFmt,
		raftPort: raftPort,
		logger:   logger,
	}, nil
}

// ---------------------------------
// NodeProvider Implementation Start
// ---------------------------------

// Index returns the index of current node.
func (p *K8SNodeProvider) Index() (int, error) {
	return p.idx, nil
}

// RaftAddr returns the network address of node (with the given index) providing Raft service.
func (p *K8SNodeProvider) RaftAddr(index int) (string, error) {
	return fmt.Sprintf(p.addrFmt, index, p.raftPort), nil
}

// Size returns the number of nodes in the cluster.
func (p *K8SNodeProvider) Size() (int, error) {
	return -1, nil
}

// ---------------------------------
// NodeProvider Implementation End
// ---------------------------------

func getClusterDomain() (string, error) {
	return "cluster.local", nil
}
