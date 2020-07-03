package cluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/pb"
)

// Environment variables.
const (
	envPodName     = "RAWKV_POD_NAME"
	envServiceName = "RAWKV_SERVICE_NAME"
	envNamespace   = "RAWKV_NAMESPACE"
	envStoragePort = "RAWKV_STORAGE_PORT"
	envRaftPort    = "RAWKV_RAFT_PORT"
)

// Assert *KubeMeta implements Meta.
var _ Meta = (*KubeMeta)(nil)

// KubeMeta implements Meta interface for a kubernetes cluster.
// An example of cluster config can be found at kubernetes/rawkv.yaml.
type KubeMeta struct {
	logger         *logging.Logger
	id             int32              // current node id
	size           int32              // number of nodes in the cluster
	storageClients []pb.StorageClient // storage grpc clients to communicate with other nodes (indexed by node id)
	raftClients    []pb.RaftClient    // raft grpc clients to communicate with other nodes (indexed by node id)
}

// NewKubeMeta instantiates a new KubeMeta.
func NewKubeMeta(logLevel int) (*KubeMeta, error) {

	km := &KubeMeta{
		logger: logging.New(logLevel),
	}

	if err := km.init(); err != nil {
		return nil, fmt.Errorf("Initialize kubernetes cluster meta failed | err=[%w]", err)
	}

	km.logger.Info("Kubernetes cluster meta created | meta=%v", km)
	return km, nil
}

func (km *KubeMeta) String() string {
	return fmt.Sprintf("[id=%v | size=%v]", km.id, km.size)
}

func (km *KubeMeta) init() error {

	// get pod name
	podName := os.Getenv(envPodName)
	if len(podName) == 0 {
		return fmt.Errorf("Get pod name failed | env=%v | val=%v", envPodName, podName)
	}
	km.logger.Info("Read pod name env | val=%v", podName)

	// get current node id
	pos := strings.LastIndex(podName, "-")
	if pos < 0 || pos == len(podName) {
		return fmt.Errorf("Invalid pod name | podName=%v", podName)
	}
	id, err := strconv.Atoi(podName[pos+1:])
	if err != nil || id < 0 {
		return fmt.Errorf("Invalid node id | podName=%v | err=[%w]", podName, err)
	}
	km.id = int32(id)

	// get service name
	serviceName := os.Getenv(envServiceName)
	if len(serviceName) == 0 {
		return fmt.Errorf("Get service name failed | env=%v | val=%v", envServiceName, serviceName)
	}
	km.logger.Info("Read service name env | val=%v", serviceName)

	// get namespace
	namespace := os.Getenv(envNamespace)
	if len(namespace) == 0 {
		return fmt.Errorf("Get namespace failed | env=%v | val=%v", envNamespace, namespace)
	}
	km.logger.Info("Read namespace env | val=%v", namespace)

	// init cluster size
	if err := km.initSize(namespace); err != nil {
		return fmt.Errorf("Initialize cluster size failed | err=[%w]", err)
	}

	// create address format
	// reference: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#stable-network-id
	addrFmt := fmt.Sprintf("%s.%s.%s.%s.%s:%s",
		podName[:pos+1]+"%v", serviceName, namespace, "svc", "cluster.local", "%v")

	// get port number of storage grpc server
	storagePort := os.Getenv(envStoragePort)
	if len(storagePort) == 0 {
		return fmt.Errorf("Get storage port failed | env=%v | val=%v", envStoragePort, storagePort)
	}
	km.logger.Info("Read storage port env | val=%v", storagePort)

	// get port number of raft grpc server
	raftPort := os.Getenv(envRaftPort)
	if len(raftPort) == 0 {
		return fmt.Errorf("Get raft port failed | env=%v | val=%v", envRaftPort, raftPort)
	}
	km.logger.Info("Read raft port env | val=%v", raftPort)

	// establish grpc connections with other storage servers in the cluster
	if err := km.initStorageClients(addrFmt, storagePort); err != nil {
		return fmt.Errorf("Create storage grpc clients failed | err=[%w]", err)
	}

	// establish grpc connections with other raft servers in the cluster
	if err := km.initRaftClients(addrFmt, raftPort); err != nil {
		return fmt.Errorf("Create raft grpc clients failed | err=[%w]", err)
	}

	return nil
}

func (km *KubeMeta) initSize(namespace string) error {
	for {
		config, err := rest.InClusterConfig()
		if err != nil {
			return fmt.Errorf("Create in-cluster config failed | namespace=%v | err=[%w]", namespace, err)
		}

		clients, err := kubernetes.NewForConfig(config)
		if err != nil {
			return fmt.Errorf("Create client set failed | namespace=%v | err=[%w]", namespace, err)
		}

		pods, err := clients.CoreV1().Pods(namespace).List(metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("List pods info failed | namespace=%v | err=[%w]", namespace, err)
		}

		km.size = int32(len(pods.Items))
		if km.size >= 3 {
			break
		}

		km.logger.Warn("Require at least three nodes in the cluster for fault tolerance. Retry after 1 second"+
			" | meta=%v", km)
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (km *KubeMeta) initStorageClients(addrFmt string, storagePort string) error {
	km.storageClients = make([]pb.StorageClient, km.size)
	for id := int32(0); id < km.size; id++ {
		if id != km.id {
			targetAddr := fmt.Sprintf(addrFmt, id, storagePort)
			conn, err := grpc.Dial(targetAddr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				return fmt.Errorf("Connect storage grpc server failed | targetAddr=%v | err=[%w]", targetAddr, err)
			}
			km.storageClients[id] = pb.NewStorageClient(conn)
		}
	}
	return nil
}

func (km *KubeMeta) initRaftClients(addrFmt string, raftPort string) error {
	km.raftClients = make([]pb.RaftClient, km.size)
	for id := int32(0); id < km.size; id++ {
		if id != km.id {
			targetAddr := fmt.Sprintf(addrFmt, id, raftPort)
			conn, err := grpc.Dial(targetAddr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				return fmt.Errorf("Connect raft grpc server failed | targetAddr=%v | err=[%w]", targetAddr, err)
			}
			km.raftClients[id] = pb.NewRaftClient(conn)
		}
	}
	return nil
}

// ---------------------------
// NodeProvider Implementation
// ---------------------------

// NodeIDNil returns the nil value of node id.
func (km *KubeMeta) NodeIDNil() int32 {
	return -1
}

// NodeIDSelf returns the id of current node.
func (km *KubeMeta) NodeIDSelf() int32 {
	return km.id
}

// Size returns the number of nodes in the cluster.
func (km *KubeMeta) Size() int32 {
	return km.size
}

// StorageClient returns the storage grpc client to communicate with the node with given id.
func (km *KubeMeta) StorageClient(id int32) pb.StorageClient {
	return km.storageClients[id]
}

// RaftClient returns the raft grpc client to communicate with the node with given id.
func (km *KubeMeta) RaftClient(id int32) pb.RaftClient {
	return km.raftClients[id]
}
