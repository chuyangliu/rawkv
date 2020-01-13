package main

import (
	"flag"

	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/node/server"
	"github.com/chuyangliu/rawkv/pkg/store"
)

var (
	logger = logging.New(logging.LevelInfo)
)

func main() {
	// parse command-line flags
	storageAddr := flag.String("storageaddr", "127.0.0.1:8000", "Address for storage server to listen.")
	rootdir := flag.String("rootdir", "./server-root", "Root directory to persist data.")
	flushThresh := flag.Uint64("flushthresh", uint64(1)<<25, "Threshold in bytes to flush MemStore.")
	blkSize := flag.Uint64("blocksize", uint64(1)<<18, "Block size in bytes to persist FileStore.")
	flag.Parse()
	// start node server
	svr := server.New(*rootdir, store.KVLen(*flushThresh), store.KVLen(*blkSize))
	if err := svr.Serve(*storageAddr); err != nil {
		logger.Error("Failed to start server | err=%v", err)
	}
}
