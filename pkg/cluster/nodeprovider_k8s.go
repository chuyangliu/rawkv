package cluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

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
	id        int32  // current node id
	namespace string // cluster namespace
	addrFmt   string // network address format for nodes
	raftPort  string // port number for raft service
	logger    *logging.Logger
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
	idStr := podName[pos+1:]
	id, err := strconv.Atoi(idStr)
	if err != nil || id < 0 {
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

	logger.Info("Kubernetes node provider created | id=%v | namespace=%v | addrFmt=%v | raftPort=%v",
		id, namespace, addrFmt, raftPort)

	return &K8SNodeProvider{
		id:        int32(id),
		namespace: namespace,
		addrFmt:   addrFmt,
		raftPort:  raftPort,
		logger:    logger,
	}, nil
}

// ---------------------------
// NodeProvider Implementation
// ---------------------------

// ID returns the id of current node.
func (p *K8SNodeProvider) ID() (int32, error) {
	return p.id, nil
}

// RaftAddr returns the network address of node (with the given id) providing Raft service.
func (p *K8SNodeProvider) RaftAddr(id int32) (string, error) {
	return fmt.Sprintf(p.addrFmt, id, p.raftPort), nil
}

// Size returns the number of nodes in the cluster.
func (p *K8SNodeProvider) Size() (int32, error) {
	clients, err := newClients()
	if err != nil {
		return -1, fmt.Errorf("Create k8s clients failed | err=[%w]", err)
	}
	pods, err := clients.CoreV1().Pods(p.namespace).List(metav1.ListOptions{})
	if err != nil {
		return -1, fmt.Errorf("List pods info failed | err=[%w]", err)
	}
	return int32(len(pods.Items)), nil
}

func getClusterDomain() (string, error) {
	return "cluster.local", nil
}

func newClients() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("Create in-cluster config failed | err=[%w]", err)
	}
	clients, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("Create client set failed | err=[%w]", err)
	}
	return clients, nil
}
