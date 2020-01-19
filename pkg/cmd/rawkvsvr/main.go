package main

import (
	"flag"

	"github.com/chuyangliu/rawkv/pkg/server"
	"github.com/chuyangliu/rawkv/pkg/store"
)

func main() {
	storageAddr := flag.String("storageaddr", "127.0.0.1:5640", "Address for storage server to listen.")
	raftAddr := flag.String("raftaddr", "127.0.0.1:5641", "Address for raft server to listen.")
	rootdir := flag.String("rootdir", "./rawkv-root", "Root directory to persist data.")
	flushThresh := flag.Uint64("flushthresh", uint64(1)<<25, "Threshold in bytes to flush MemStore.")
	blkSize := flag.Uint64("blocksize", uint64(1)<<18, "Block size in bytes to persist FileStore.")
	level := flag.Int("loglevel", 1, "Log level (0/1/2/3 => Debug/Info/Warn/Error).")
	flag.Parse()

	svr, err := server.New(*rootdir, store.KVLen(*flushThresh), store.KVLen(*blkSize), *level)
	if err != nil {
		panic(err)
	}

	if err := svr.Serve(*storageAddr, *raftAddr); err != nil {
		panic(err)
	}
}
