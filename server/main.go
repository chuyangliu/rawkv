package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/chuyangliu/rawkv/logging"
	"github.com/chuyangliu/rawkv/server/storage"
	"github.com/chuyangliu/rawkv/store"
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

	// create gRPC server
	gRPCSvr := grpc.NewServer()

	// create listener for storage server
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *storageHost, *storagePort))
	if err != nil {
		logger.Error("Listen failed | storagehost=%v | storageport=%v | err=%v", *storageHost, *storagePort, err)
		return
	}

	// create storage server
	if err := os.MkdirAll(*rootdir, 0777); err != nil {
		logger.Error("Creae root directory failed | rootdir=%v", *rootdir)
		return
	}
	storageSvr := storage.NewServer(*rootdir, store.KVLen(*flushThresh), store.KVLen(*blkSize))
	storage.RegisterStorageServer(gRPCSvr, storageSvr)
	logger.Info("Storage server registered | host=%v | port=%v", *storageHost, *storagePort)

	// start
	logger.Info("gRPC server started")
	gRPCSvr.Serve(listener)
}
