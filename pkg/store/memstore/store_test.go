package memstore

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/chuyangliu/rawkv/pkg/store"
	"github.com/stretchr/testify/assert"
)

type checkExistResult struct {
	key   store.Key
	exist bool
}

func TestBasic(t *testing.T) {
	max := 1000
	sizeMeta := (store.KVLenSize*2 + store.KStatSize) * store.KVLen(max)
	sizeKV := store.KVLen(0)

	// create data
	data := make([]string, max)
	for i := 0; i < max; i++ {
		data[i] = strconv.Itoa(i)
		sizeKV += store.KVLen(len(data[i]))
	}
	sort.Strings(data)

	s := New()

	// put
	for _, v := range data {
		s.Put(store.Key(v), store.Value(v))
	}

	// size
	if !assert.Equal(t, sizeMeta+sizeKV*2, s.Size()) {
		panic(nil)
	}

	// get
	for _, v := range data {
		entryExpect := store.Entry{
			Key:  store.Key(v),
			Val:  store.Value(v),
			Stat: store.KStatPut,
		}
		entry := s.Get(entryExpect.Key)
		if !assert.NotNil(t, entry) || !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}

	// get all
	for i, entry := range s.Entries() {
		entryExpect := store.Entry{
			Key:  store.Key(data[i]),
			Val:  store.Value(data[i]),
			Stat: store.KStatPut,
		}
		if !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}

	// del
	for _, v := range data {
		s.Del(store.Key(v))

	}

	// size
	if !assert.Equal(t, sizeMeta+sizeKV, s.Size()) {
		panic(nil)
	}

	// get
	for _, v := range data {
		entryExpect := store.Entry{
			Key:  store.Key(v),
			Val:  "",
			Stat: store.KStatDel,
		}
		entry := s.Get(entryExpect.Key)
		if !assert.NotNil(t, entry) || !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}

	// get all
	for i, entry := range s.Entries() {
		entryExpect := store.Entry{
			Key:  store.Key(data[i]),
			Val:  "",
			Stat: store.KStatDel,
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
	sizeMeta := (store.KVLenSize*2 + store.KStatSize) * store.KVLen(max)
	sizeKV := store.KVLen(0)

	// create data
	data := make([]string, max)
	for i := 0; i < max; i++ {
		data[i] = strconv.Itoa(i)
		sizeKV += store.KVLen(len(data[i]))
	}
	sort.Strings(data)

	s := New()

	// put
	finishes := make(chan bool, numWorkers)
	for i := 0; i < max; i += step {
		go putData(s, data[i:i+step], finishes)
	}
	for i := 0; i < numWorkers; i++ {
		<-finishes
	}

	// size
	if !assert.Equal(t, sizeMeta+sizeKV*2, s.Size()) {
		panic(nil)
	}

	// get
	results := make(chan checkExistResult, max)
	for i := 0; i < max; i += step {
		go checkDataExist(s, data[i:i+step], results)
	}
	for i := 0; i < max; i++ {
		if result := <-results; !assert.True(t, result.exist) {
			panic(fmt.Sprintf("key %v doesn't exist", result.key))
		}
	}

	// get all
	for i, entry := range s.Entries() {
		entryExpect := store.Entry{
			Key:  store.Key(data[i]),
			Val:  store.Value(data[i]),
			Stat: store.KStatPut,
		}
		if !assert.Equal(t, entryExpect, *entry) {
			panic(nil)
		}
	}

	// del
	for i := 0; i < max; i += step {
		go delData(s, data[i:i+step], finishes)
	}
	for i := 0; i < numWorkers; i++ {
		<-finishes
	}

	// size
	if !assert.Equal(t, sizeMeta+sizeKV, s.Size()) {
		panic(nil)
	}

	// get
	for i := 0; i < max; i += step {
		go checkDataExist(s, data[i:i+step], results)
	}
	for i := 0; i < max; i++ {
		if result := <-results; !assert.False(t, result.exist) {
			panic(fmt.Sprintf("key %v not deleted", result.key))
		}
	}

	// get all
	for i, entry := range s.Entries() {
		entryExpect := store.Entry{
			Key:  store.Key(data[i]),
			Val:  "",
			Stat: store.KStatDel,
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
		if entry == nil || entry.Val != store.Value(v) || entry.Stat != store.KStatPut {
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