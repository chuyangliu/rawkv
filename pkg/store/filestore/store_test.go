package filestore

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/store"
	"github.com/chuyangliu/rawkv/pkg/store/memstore"
	"github.com/stretchr/testify/assert"
)

type checkExistResult struct {
	key   store.Key
	exist bool
}

func TestBasic(t *testing.T) {
	max := 1000
	blockSize := store.KVLen(4096)
	path := "./filestore.test"

	// Create data.
	data := make([]string, max)
	for i := 0; i < max; i++ {
		data[i] = strconv.Itoa(i)
	}
	sort.Strings(data)

	// Create MemStore.
	ms := memstore.New(logging.LevelDebug)
	for _, v := range data {
		ms.Put(store.Key(v), store.Value(v))
	}

	// Create FileStore from MemStore.
	fs, err := New(logging.LevelDebug, path, ms)
	if !assert.NoError(t, err) {
		panic(nil)
	}

	// Get.
	for _, v := range data {
		entryExpect := store.Entry{
			Key:    store.Key(v),
			Value:  store.Value(v),
			Status: store.StatusPut,
		}
		entry, err := fs.Get(entryExpect.Key)
		if !assert.NoError(t, err) || !assert.NotNil(t, entry) || !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}

	// Flush.
	if err := fs.Flush(blockSize); !assert.NoError(t, err) {
		panic(nil)
	}

	// Get.
	for _, v := range data {
		entryExpect := store.Entry{
			Key:    store.Key(v),
			Value:  store.Value(v),
			Status: store.StatusPut,
		}
		entry, err := fs.Get(entryExpect.Key)
		if !assert.NoError(t, err) || !assert.NotNil(t, entry) || !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}

	// Create FileStore from disk.
	fs, err = New(logging.LevelDebug, path, nil)
	if !assert.NoError(t, err) {
		panic(nil)
	}

	// Get.
	for _, v := range data {
		entryExpect := store.Entry{
			Key:    store.Key(v),
			Value:  store.Value(v),
			Status: store.StatusPut,
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
	blockSize := store.KVLen(4096)
	path := "./filestore.test"

	// Create data.
	data := make([]string, max)
	for i := 0; i < max; i++ {
		data[i] = strconv.Itoa(i)
	}
	sort.Strings(data)

	// Create MemStore.
	ms := memstore.New(logging.LevelDebug)
	for _, v := range data {
		ms.Put(store.Key(v), store.Value(v))
	}

	// Create FileStore from MemStore.
	fs, err := New(logging.LevelDebug, path, ms)
	if !assert.NoError(t, err) {
		panic(nil)
	}

	// Get in background.
	results := make(chan checkExistResult, max)
	for i := 0; i < max; i += step {
		go checkDataExist(fs, data[i:i+step], results)
	}

	// Flush.
	sleepRand()
	if err := fs.Flush(blockSize); !assert.NoError(t, err) {
		panic(nil)
	}

	// Check existence.
	for i := 0; i < max; i++ {
		if result := <-results; !assert.True(t, result.exist) {
			panic(fmt.Sprintf("key %v doesn't exist", result.key))
		}
	}

	// Create FileStore from disk.
	fs, err = New(logging.LevelDebug, path, nil)
	if !assert.NoError(t, err) {
		panic(nil)
	}

	// Get in background.
	for i := 0; i < max; i += step {
		go checkDataExist(fs, data[i:i+step], results)
	}

	// Check existence.
	for i := 0; i < max; i++ {
		if result := <-results; !assert.True(t, result.exist) {
			panic(fmt.Sprintf("key %v doesn't exist", result.key))
		}
	}
}

func checkDataExist(fs *Store, data []string, results chan checkExistResult) {
	sleepRand()
	for _, v := range data {
		entryExpect := store.Entry{
			Key:    store.Key(v),
			Value:  store.Value(v),
			Status: store.StatusPut,
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
	time.Sleep(time.Duration(rand.Intn(10000)) * time.Microsecond)
}
