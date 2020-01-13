package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/chuyangliu/rawkv/pkg/rpc"
	"github.com/chuyangliu/rawkv/pkg/store"
	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	max := 1000
	storageHost := "127.0.0.1"
	storagePort := 8000
	rootdir := "./node.test"
	flushThresh := store.KVLen(1024 * 10)
	blkSize := store.KVLen(1024 * 2)

	// start server
	go start(storageHost, storagePort, rootdir, flushThresh, blkSize)
	time.Sleep(time.Duration(1) * time.Second) // wait server starts

	// create client
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", storageHost, storagePort), grpc.WithInsecure())
	if !assert.NoError(t, err) {
		panic(nil)
	}
	defer conn.Close()
	client := rpc.NewStorageClient(conn)

	// create data
	data := make([]string, max)
	for i := 0; i < max; i++ {
		data[i] = strconv.Itoa(i)
	}
	sort.Strings(data)

	// put
	for _, v := range data {
		req := &rpc.PutReq{Key: []byte(v), Val: []byte(v)}
		if _, err := client.Put(context.Background(), req); !assert.NoError(t, err) {
			panic(nil)
		}
	}

	// get
	for _, v := range data {
		req := &rpc.GetReq{Key: []byte(v)}
		resp, err := client.Get(context.Background(), req)
		if !assert.NoError(t, err) || !assert.True(t, resp.Found) || !assert.Equal(t, []byte(v), resp.Val) {
			panic(nil)
		}
	}

	// del
	for _, v := range data {
		req := &rpc.DelReq{Key: []byte(v)}
		if _, err := client.Del(context.Background(), req); !assert.NoError(t, err) {
			panic(nil)
		}
	}

	// get
	for _, v := range data {
		req := &rpc.GetReq{Key: []byte(v)}
		resp, err := client.Get(context.Background(), req)
		if !assert.NoError(t, err) || !assert.False(t, resp.Found) {
			panic(nil)
		}
	}
}
