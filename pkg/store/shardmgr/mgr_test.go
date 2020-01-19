package shardmgr

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/chuyangliu/rawkv/pkg/logging"
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
	rootdir := "./shardmgr.test"
	flushThresh := store.KVLen(1024 * 10)
	blkSize := store.KVLen(1024 * 2)

	// create root directory
	if err := os.MkdirAll(rootdir, 0777); !assert.NoError(t, err) {
		panic(nil)
	}

	// create data
	data := make([]string, max)
	for i := 0; i < max; i++ {
		data[i] = strconv.Itoa(i)
	}
	sort.Strings(data)

	// create Manager
	m := New(rootdir, flushThresh, blkSize, logging.LevelDebug)

	// put
	for _, v := range data {
		if err := m.Put(store.Key(v), store.Value(v)); !assert.NoError(t, err) {
			panic(nil)
		}
	}

	// get
	for _, v := range data {
		val, found, err := m.Get(store.Key(v))
		if !assert.NoError(t, err) || !assert.True(t, found) || !assert.Equal(t, store.Value(v), val) {
			panic(nil)
		}
	}

	// del
	for _, v := range data {
		if err := m.Del(store.Key(v)); !assert.NoError(t, err) {
			panic(nil)
		}
	}

	// get
	for _, v := range data {
		_, found, err := m.Get(store.Key(v))
		if !assert.NoError(t, err) || !assert.False(t, found) {
			panic(nil)
		}
	}
}

func TestConcurrency(t *testing.T) {
	max := 1000
	step := 100
	rootdir := "./shardmgr.test"
	flushThresh := store.KVLen(1024 * 10)
	blkSize := store.KVLen(1024 * 2)

	// create root directory
	if err := os.MkdirAll(rootdir, 0777); !assert.NoError(t, err) {
		panic(nil)
	}

	// create data
	data := make([]string, max)
	for i := 0; i < max; i++ {
		data[i] = strconv.Itoa(i)
	}
	sort.Strings(data)

	// create Shard
	m := New(rootdir, flushThresh, blkSize, logging.LevelDebug)

	// put
	putResults := make(chan putDelResult, max)
	for i := 0; i < max; i += step {
		go putData(m, data[i:i+step], putResults)
	}
	for i := 0; i < max; i++ {
		if result := <-putResults; !assert.NoError(t, result.err) {
			panic(fmt.Sprintf("Put %v failed", result.key))
		}
	}

	// get
	getResults := make(chan checkExistResult, max)
	for i := 0; i < max; i += step {
		go checkDataExist(m, data[i:i+step], getResults)
	}
	for i := 0; i < max; i++ {
		if result := <-getResults; !assert.NoError(t, result.err) || !assert.True(t, result.exist) {
			panic(fmt.Sprintf("Get %v failed", result.key))
		}
	}

	// del
	delResults := make(chan putDelResult, max)
	for i := 0; i < max; i += step {
		go delData(m, data[i:i+step], delResults)
	}
	for i := 0; i < max; i++ {
		if result := <-delResults; !assert.NoError(t, result.err) {
			panic(fmt.Sprintf("Del %v failed", result.key))
		}
	}

	// get
	for i := 0; i < max; i += step {
		go checkDataExist(m, data[i:i+step], getResults)
	}
	for i := 0; i < max; i++ {
		if result := <-getResults; !assert.NoError(t, result.err) || !assert.False(t, result.exist) {
			panic(fmt.Sprintf("Del %v failed", result.key))
		}
	}
}

func putData(m *Manager, data []string, results chan putDelResult) {
	sleepRand()
	for _, v := range data {
		err := m.Put(store.Key(v), store.Value(v))
		results <- putDelResult{key: store.Key(v), err: err}
	}
}

func delData(m *Manager, data []string, results chan putDelResult) {
	sleepRand()
	for _, v := range data {
		err := m.Del(store.Key(v))
		results <- putDelResult{key: store.Key(v), err: err}
	}
}

func checkDataExist(m *Manager, data []string, results chan checkExistResult) {
	sleepRand()
	for _, v := range data {
		val, found, err := m.Get(store.Key(v))
		results <- checkExistResult{
			key:   store.Key(v),
			exist: err == nil && found && val == store.Value(v),
			err:   err,
		}
	}
}

func sleepRand() {
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Duration(rand.Intn(3000)) * time.Microsecond)
}
