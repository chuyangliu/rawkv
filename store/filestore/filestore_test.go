package filestore

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/chuyangliu/rawkv/store"
	"github.com/chuyangliu/rawkv/store/memstore"
	"github.com/stretchr/testify/assert"
)

type checkExistResult struct {
	key   store.Key
	exist bool
}

func TestBasic(t *testing.T) {
	max := 1000
	blkSize := store.KVLen(4096)
	path := "./filestore.test"

	// create data
	data := make([]string, max)
	for i := 0; i < max; i++ {
		data[i] = strconv.Itoa(i)
	}
	sort.Strings(data)

	// create MemStore
	ms := memstore.New()
	for _, v := range data {
		ms.Put(store.Key(v), store.Value(v))
	}

	// create FileStore from MemStore
	fs := New(path, ms)

	// get
	for _, v := range data {
		entryExpect := store.Entry{
			Key:  store.Key(v),
			Val:  store.Value(v),
			Stat: store.KStatPut,
		}
		entry, err := fs.Get(entryExpect.Key)
		if !assert.NoError(t, err) || !assert.NotNil(t, entry) || !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}

	// flush
	if err := fs.Flush(blkSize); !assert.NoError(t, err) {
		panic(nil)
	}

	// get
	for _, v := range data {
		entryExpect := store.Entry{
			Key:  store.Key(v),
			Val:  store.Value(v),
			Stat: store.KStatPut,
		}
		entry, err := fs.Get(entryExpect.Key)
		if !assert.NoError(t, err) || !assert.NotNil(t, entry) || !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}

	// create FileStore from disk
	fs = New(path, nil)

	// get
	for _, v := range data {
		entryExpect := store.Entry{
			Key:  store.Key(v),
			Val:  store.Value(v),
			Stat: store.KStatPut,
		}
		entry, err := fs.Get(entryExpect.Key)
		if !assert.NoError(t, err) || !assert.NotNil(t, entry) || !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}
}

func TestConcurrency(t *testing.T) {
	max := 1000
	step := 100
	blkSize := store.KVLen(4096)
	path := "./filestore.test"

	// create data
	data := make([]string, max)
	for i := 0; i < max; i++ {
		data[i] = strconv.Itoa(i)
	}
	sort.Strings(data)

	// create MemStore
	ms := memstore.New()
	for _, v := range data {
		ms.Put(store.Key(v), store.Value(v))
	}

	// create FileStore from MemStore
	fs := New(path, ms)

	// get in background
	results := make(chan checkExistResult, max)
	for i := 0; i < max; i += step {
		go checkDataExist(fs, data[i:i+step], results)
	}

	// flush
	sleepRand()
	if err := fs.Flush(blkSize); !assert.NoError(t, err) {
		panic(nil)
	}

	// check existence
	for i := 0; i < max; i++ {
		if result := <-results; !assert.True(t, result.exist) {
			panic(fmt.Sprintf("key %v doesn't exist", result.key))
		}
	}
}

func checkDataExist(fs *FileStore, data []string, results chan checkExistResult) {
	sleepRand()
	for _, v := range data {
		entryExpect := store.Entry{
			Key:  store.Key(v),
			Val:  store.Value(v),
			Stat: store.KStatPut,
		}
		entry, err := fs.Get(entryExpect.Key)
		if err != nil || !reflect.DeepEqual(entryExpect, *entry) {
			results <- checkExistResult{key: store.Key(v), exist: false}
		} else {
			results <- checkExistResult{key: store.Key(v), exist: true}
		}
	}
}

func sleepRand() {
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Duration(rand.Intn(5000)) * time.Microsecond)
}
