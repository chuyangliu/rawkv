package memstore

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/chuyangliu/rawkv/store"
	"github.com/stretchr/testify/assert"
)

type concurrentGetEntry struct {
	key   int
	exist bool
}

func TestBasic(t *testing.T) {
	const max = 1000
	const metaSize = (store.KVLenSize*2 + store.KStatSize) * max

	ms := New()
	kvSize := store.KVLen(0)

	// put
	for i := 0; i < max; i++ {
		ms.put(store.Key(i), store.Value(i))
		kvSize += store.KVLen(len(store.Key(i)))
	}

	// size
	if !assert.Equal(t, metaSize+kvSize*2, ms.Size()) {
		panic(nil)
	}

	// get
	for i := 0; i < max; i++ {
		val, found := ms.get(store.Key(i))

		if !assert.True(t, found) {
			panic(nil)
		}
		if !assert.Equal(t, store.Value(i), val) {
			panic(nil)
		}
	}

	// get all
	for i, entry := range ms.Entries() {
		if !assert.Equal(t, store.Key(i), entry.Key) {
			panic(nil)
		}
		if !assert.Equal(t, store.Value(i), entry.Val) {
			panic(nil)
		}
		if !assert.Equal(t, store.KStatPut, entry.Stat) {
			panic(nil)
		}
	}

	// del
	for i := 0; i < max; i++ {
		ms.del(store.Key(i))
	}

	// size
	if !assert.Equal(t, metaSize+kvSize, ms.Size()) {
		panic(nil)
	}

	// get
	for i := 0; i < max; i++ {
		_, found := ms.get(store.Key(i))

		if !assert.False(t, found) {
			panic(nil)
		}
	}

	// get all
	for i, entry := range ms.Entries() {
		if !assert.Equal(t, store.Key(i), entry.Key) {
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
	const max, step = 1000, 100
	const numWorkers = max / step
	const metaSize = (store.KVLenSize*2 + store.KStatSize) * max

	ms := New()

	// put
	kvSizes := make(chan store.KVLen, numWorkers)
	for i := 0; i < max; i += step {
		go putData(ms, i, i+step, kvSizes)
	}

	// size
	kvSize := store.KVLen(0)
	for i := 0; i < numWorkers; i++ {
		kvSize += <-kvSizes
	}
	if !assert.Equal(t, metaSize+kvSize*2, ms.Size()) {
		panic(nil)
	}

	// get
	entries := make(chan concurrentGetEntry, max)
	for i := 0; i < max; i += step {
		go checkDataExist(ms, i, i+step, entries)
	}
	for i := 0; i < max; i++ {
		if entry := <-entries; !assert.True(t, entry.exist) {
			panic(fmt.Sprintf("key %v doesn't exist", entry.key))
		}
	}

	// get all
	for i, entry := range ms.Entries() {
		if !assert.Equal(t, store.Key(i), entry.Key) {
			panic(nil)
		}
		if !assert.Equal(t, store.Value(i), entry.Val) {
			panic(nil)
		}
		if !assert.Equal(t, store.KStatPut, entry.Stat) {
			panic(nil)
		}
	}

	// del
	finishes := make(chan bool, numWorkers)
	for i := 0; i < max; i += step {
		go delData(ms, i, i+step, finishes)
	}
	for i := 0; i < numWorkers; i++ {
		<-finishes
	}

	// size
	if !assert.Equal(t, metaSize+kvSize, ms.Size()) {
		panic(nil)
	}

	// get
	for i := 0; i < max; i += step {
		go checkDataExist(ms, i, i+step, entries)
	}
	for i := 0; i < max; i++ {
		if entry := <-entries; !assert.False(t, entry.exist) {
			panic(fmt.Sprintf("key %v not deleted", entry.key))
		}
	}

	// get all
	for i, entry := range ms.Entries() {
		if !assert.Equal(t, store.Key(i), entry.Key) {
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

func putData(ms *MemStore, beg int, end int, kvSizes chan store.KVLen) {
	sleepRand()
	kvSize := store.KVLen(0)
	for i := beg; i < end; i++ {
		ms.put(store.Key(i), store.Value(i))
		kvSize += store.KVLen(len(store.Key(i)))
	}
	kvSizes <- kvSize
}

func delData(ms *MemStore, beg int, end int, finishes chan bool) {
	sleepRand()
	for i := beg; i < end; i++ {
		ms.del(store.Key(i))
	}
	finishes <- true
}

func checkDataExist(ms *MemStore, beg int, end int, entries chan concurrentGetEntry) {
	sleepRand()
	for i := beg; i < end; i++ {
		val, found := ms.get(store.Key(i))
		if !found || val != store.Value(i) {
			entries <- concurrentGetEntry{key: i, exist: false}
		} else {
			entries <- concurrentGetEntry{key: i, exist: true}
		}
	}
}

func sleepRand() {
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Duration(rand.Intn(3000)) * time.Microsecond)
}
