package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/chuyangliu/rawkv/pkg/rpc"
	"github.com/chuyangliu/rawkv/pkg/store"
	"github.com/stretchr/testify/assert"
)

type putDelResult struct {
	key store.Key
	err error
}

type checkExistResult struct {
	key   store.Key
	exist bool
	err   error
}

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

func TestConcurrency(t *testing.T) {
	max := 1000
	step := 100
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
	putResults := make(chan putDelResult, max)
	for i := 0; i < max; i += step {
		go putData(client, data[i:i+step], putResults)
	}
	for i := 0; i < max; i++ {
		if result := <-putResults; !assert.NoError(t, result.err) {
			panic(fmt.Sprintf("Put %v failed", result.key))
		}
	}

	// get
	getResults := make(chan checkExistResult, max)
	for i := 0; i < max; i += step {
		go checkDataExist(client, data[i:i+step], getResults)
	}
	for i := 0; i < max; i++ {
		if result := <-getResults; !assert.NoError(t, result.err) || !assert.True(t, result.exist) {
			panic(fmt.Sprintf("Get %v failed", result.key))
		}
	}

	// del
	delResults := make(chan putDelResult, max)
	for i := 0; i < max; i += step {
		go delData(client, data[i:i+step], delResults)
	}
	for i := 0; i < max; i++ {
		if result := <-delResults; !assert.NoError(t, result.err) {
			panic(fmt.Sprintf("Del %v failed", result.key))
		}
	}

	// get
	for i := 0; i < max; i += step {
		go checkDataExist(client, data[i:i+step], getResults)
	}
	for i := 0; i < max; i++ {
		if result := <-getResults; !assert.NoError(t, result.err) || !assert.False(t, result.exist) {
			panic(fmt.Sprintf("Del %v failed", result.key))
		}
	}
}

func putData(c rpc.StorageClient, data []string, results chan putDelResult) {
	sleepRand()
	for _, v := range data {
		req := &rpc.PutReq{Key: []byte(v), Val: []byte(v)}
		_, err := c.Put(context.Background(), req)
		results <- putDelResult{key: store.Key(v), err: err}
	}
}

func delData(c rpc.StorageClient, data []string, results chan putDelResult) {
	sleepRand()
	for _, v := range data {
		req := &rpc.DelReq{Key: []byte(v)}
		_, err := c.Del(context.Background(), req)
		results <- putDelResult{key: store.Key(v), err: err}
	}
}

func checkDataExist(c rpc.StorageClient, data []string, results chan checkExistResult) {
	sleepRand()
	for _, v := range data {
		req := &rpc.GetReq{Key: []byte(v)}
		resp, err := c.Get(context.Background(), req)
		results <- checkExistResult{
			key:   store.Key(v),
			exist: err == nil && resp.Found && bytes.Compare(resp.Val, []byte(v)) == 0,
			err:   err,
		}
	}
}

func sleepRand() {
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Duration(rand.Intn(3000)) * time.Microsecond)
}
