package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/node/server"
	"github.com/chuyangliu/rawkv/pkg/rpc"
	"github.com/chuyangliu/rawkv/pkg/store"
)

var (
	logger = logging.New(logging.LevelInfo)
)

// Command-line flags
var (
	storageHost = flag.String("storagehost", "127.0.0.1", "Hostname for storage server to listen.")
	storagePort = flag.Int("storageport", 8000, "Port for storage server to listen.")
	rootdir     = flag.String("rootdir", "./server-root", "Root directory to persist data.")
	flushThresh = flag.Uint64("flushthresh", uint64(1)<<25, "Threshold in bytes to flush MemStore.")
	blkSize     = flag.Uint64("blocksize", uint64(1)<<18, "Block size in bytes to persist FileStore.")
)

func main() {
	// parse command-line flags
	flag.Parse()

	// create listener for storage RPC service
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *storageHost, *storagePort))
	if err != nil {
		logger.Error("Listen failed | storagehost=%v | storageport=%v | err=%v", *storageHost, *storagePort, err)
		return
	}

	// create root directory
	if err := os.MkdirAll(*rootdir, 0777); err != nil {
		logger.Error("Create root directory failed | rootdir=%v", *rootdir)
		return
	}

	// create node server
	nodeSvr := server.New(*rootdir, store.KVLen(*flushThresh), store.KVLen(*blkSize))

	// run gRPC server hosting storage RPC service
	svr := grpc.NewServer()
	rpc.RegisterStorageServer(svr, nodeSvr)
	logger.Info("Storage server started | host=%v | port=%v | rootdir=%v | flushThresh=%v | blkSize=%v",
		*storageHost, *storagePort, *rootdir, *flushThresh, *blkSize)
	svr.Serve(listener)
}
