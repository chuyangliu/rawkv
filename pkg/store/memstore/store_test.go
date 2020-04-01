package memstore

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/store"
	"github.com/stretchr/testify/assert"
)

type checkExistResult struct {
	key   store.Key
	exist bool
}

func TestBasic(t *testing.T) {
	max := 1000
	sizeMeta := (store.KVLenSize*2 + store.StatusSize) * store.KVLen(max)
	sizeKV := store.KVLen(0)

	// Create data.
	data := make([]string, max)
	for i := 0; i < max; i++ {
		data[i] = strconv.Itoa(i)
		sizeKV += store.KVLen(len(data[i]))
	}
	sort.Strings(data)

	s := New(logging.LevelDebug)

	// Put.
	for _, v := range data {
		s.Put(store.Key(v), store.Value(v))
	}

	// Size.
	if !assert.Equal(t, sizeMeta+sizeKV*2, s.Size()) {
		panic(nil)
	}

	// Get.
	for _, v := range data {
		entryExpect := store.Entry{
			Key:    store.Key(v),
			Value:  store.Value(v),
			Status: store.StatusPut,
		}
		entry := s.Get(entryExpect.Key)
		if !assert.NotNil(t, entry) || !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}

	// Get all.
	for i, entry := range s.Entries() {
		entryExpect := store.Entry{
			Key:    store.Key(data[i]),
			Value:  store.Value(data[i]),
			Status: store.StatusPut,
		}
		if !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}

	// Del.
	for _, v := range data {
		s.Del(store.Key(v))

	}

	// Size.
	if !assert.Equal(t, sizeMeta+sizeKV, s.Size()) {
		panic(nil)
	}

	// Get.
	for _, v := range data {
		entryExpect := store.Entry{
			Key:    store.Key(v),
			Value:  "",
			Status: store.StatusDel,
		}
		entry := s.Get(entryExpect.Key)
		if !assert.NotNil(t, entry) || !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}

	// Get all.
	for i, entry := range s.Entries() {
		entryExpect := store.Entry{
			Key:    store.Key(data[i]),
			Value:  "",
			Status: store.StatusDel,
		}
		if !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}
}

func TestConcurrency(t *testing.T) {
	max := 1000
	step := 100
	numWorkers := max / step
	sizeMeta := (store.KVLenSize*2 + store.StatusSize) * store.KVLen(max)
	sizeKV := store.KVLen(0)

	// Create data.
	data := make([]string, max)
	for i := 0; i < max; i++ {
		data[i] = strconv.Itoa(i)
		sizeKV += store.KVLen(len(data[i]))
	}
	sort.Strings(data)

	s := New(logging.LevelDebug)

	// Put.
	finishes := make(chan bool, numWorkers)
	for i := 0; i < max; i += step {
		go putData(s, data[i:i+step], finishes)
	}
	for i := 0; i < numWorkers; i++ {
		<-finishes
	}

	// Size.
	if !assert.Equal(t, sizeMeta+sizeKV*2, s.Size()) {
		panic(nil)
	}

	// Get.
	results := make(chan checkExistResult, max)
	for i := 0; i < max; i += step {
		go checkDataExist(s, data[i:i+step], results)
	}
	for i := 0; i < max; i++ {
		if result := <-results; !assert.True(t, result.exist) {
			panic(fmt.Sprintf("key %v doesn't exist", result.key))
		}
	}

	// Get all.
	for i, entry := range s.Entries() {
		entryExpect := store.Entry{
			Key:    store.Key(data[i]),
			Value:  store.Value(data[i]),
			Status: store.StatusPut,
		}
		if !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}

	// Del.
	for i := 0; i < max; i += step {
		go delData(s, data[i:i+step], finishes)
	}
	for i := 0; i < numWorkers; i++ {
		<-finishes
	}

	// Size.
	if !assert.Equal(t, sizeMeta+sizeKV, s.Size()) {
		panic(nil)
	}

	// Get.
	for i := 0; i < max; i += step {
		go checkDataExist(s, data[i:i+step], results)
	}
	for i := 0; i < max; i++ {
		if result := <-results; !assert.False(t, result.exist) {
			panic(fmt.Sprintf("key %v not deleted", result.key))
		}
	}

	// Get all.
	for i, entry := range s.Entries() {
		entryExpect := store.Entry{
			Key:    store.Key(data[i]),
			Value:  "",
			Status: store.StatusDel,
		}
		if !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}
}

func putData(s *Store, data []string, finishes chan bool) {
	sleepRand()
	for _, v := range data {
		s.Put(store.Key(v), store.Value(v))
	}
	finishes <- true
}

func delData(s *Store, data []string, finishes chan bool) {
	sleepRand()
	for _, v := range data {
		s.Del(store.Key(v))
	}
	finishes <- true
}

func checkDataExist(s *Store, data []string, results chan checkExistResult) {
	sleepRand()
	for _, v := range data {
		entry := s.Get(store.Key(v))
		if entry == nil || entry.Value != store.Value(v) || entry.Status != store.StatusPut {
			results <- checkExistResult{key: store.Key(v), exist: false}
		} else {
			results <- checkExistResult{key: store.Key(v), exist: true}
		}
	}
}

func sleepRand() {
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Duration(rand.Intn(3000)) * time.Microsecond)
}
