package memstore

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/chuyangliu/rawkv/store"
	"github.com/stretchr/testify/assert"
)

type concurrentGetEntry struct {
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

	ms := New()

	// put
	for _, v := range data {
		ms.Put(store.Key(v), store.Value(v))
	}

	// size
	if !assert.Equal(t, sizeMeta+sizeKV*2, ms.Size()) {
		panic(nil)
	}

	// get
	for _, v := range data {
		val, found := ms.Get(store.Key(v))

		if !assert.True(t, found) {
			panic(nil)
		}
		if !assert.Equal(t, store.Value(v), val) {
			panic(nil)
		}
	}

	// get all
	for i, entry := range ms.Entries() {
		if !assert.Equal(t, store.Key(data[i]), entry.Key) {
			panic(nil)
		}
		if !assert.Equal(t, store.Value(data[i]), entry.Val) {
			panic(nil)
		}
		if !assert.Equal(t, store.KStatPut, entry.Stat) {
			panic(nil)
		}
	}

	// del
	for _, v := range data {
		ms.Del(store.Key(v))
	}

	// size
	if !assert.Equal(t, sizeMeta+sizeKV, ms.Size()) {
		panic(nil)
	}

	// get
	for _, v := range data {
		_, found := ms.Get(store.Key(v))

		if !assert.False(t, found) {
			panic(nil)
		}
	}

	// get all
	for i, entry := range ms.Entries() {
		if !assert.Equal(t, store.Key(data[i]), entry.Key) {
			panic(nil)
		}
		if !assert.Equal(t, store.Value(""), entry.Val) {
			panic(nil)
		}
		if !assert.Equal(t, store.KStatDel, entry.Stat) {
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

	ms := New()

	// put
	finishes := make(chan bool, numWorkers)
	for i := 0; i < max; i += step {
		go putData(ms, data[i:i+step], finishes)
	}
	for i := 0; i < numWorkers; i++ {
		<-finishes
	}

	// size
	if !assert.Equal(t, sizeMeta+sizeKV*2, ms.Size()) {
		panic(nil)
	}

	// get
	entries := make(chan concurrentGetEntry, max)
	for i := 0; i < max; i += step {
		go checkDataExist(ms, data[i:i+step], entries)
	}
	for i := 0; i < max; i++ {
		if entry := <-entries; !assert.True(t, entry.exist) {
			panic(fmt.Sprintf("key %v doesn't exist", entry.key))
		}
	}

	// get all
	for i, entry := range ms.Entries() {
		if !assert.Equal(t, store.Key(data[i]), entry.Key) {
			panic(nil)
		}
		if !assert.Equal(t, store.Value(data[i]), entry.Val) {
			panic(nil)
		}
		if !assert.Equal(t, store.KStatPut, entry.Stat) {
			panic(nil)
		}
	}

	// del
	for i := 0; i < max; i += step {
		go delData(ms, data[i:i+step], finishes)
	}
	for i := 0; i < numWorkers; i++ {
		<-finishes
	}

	// size
	if !assert.Equal(t, sizeMeta+sizeKV, ms.Size()) {
		panic(nil)
	}

	// get
	for i := 0; i < max; i += step {
		go checkDataExist(ms, data[i:i+step], entries)
	}
	for i := 0; i < max; i++ {
		if entry := <-entries; !assert.False(t, entry.exist) {
			panic(fmt.Sprintf("key %v not deleted", entry.key))
		}
	}

	// get all
	for i, entry := range ms.Entries() {
		if !assert.Equal(t, store.Key(data[i]), entry.Key) {
			panic(nil)
		}
		if !assert.Equal(t, store.Value(""), entry.Val) {
			panic(nil)
		}
		if !assert.Equal(t, store.KStatDel, entry.Stat) {
			panic(nil)
		}
	}
}

func putData(ms *MemStore, data []string, finishes chan bool) {
	sleepRand()
	for _, v := range data {
		ms.Put(store.Key(v), store.Value(v))
	}
	finishes <- true
}

func delData(ms *MemStore, data []string, finishes chan bool) {
	sleepRand()
	for _, v := range data {
		ms.Del(store.Key(v))
	}
	finishes <- true
}

func checkDataExist(ms *MemStore, data []string, entries chan concurrentGetEntry) {
	sleepRand()
	for _, v := range data {
		val, found := ms.Get(store.Key(v))
		if !found || val != store.Value(v) {
			entries <- concurrentGetEntry{key: store.Key(v), exist: false}
		} else {
			entries <- concurrentGetEntry{key: store.Key(v), exist: true}
		}
	}
}

func sleepRand() {
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Duration(rand.Intn(3000)) * time.Microsecond)
}
