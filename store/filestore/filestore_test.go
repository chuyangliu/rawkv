package filestore

import (
	"sort"
	"strconv"
	"testing"

	"github.com/chuyangliu/rawkv/store"
	"github.com/chuyangliu/rawkv/store/memstore"
	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	max := 1
	blkSize := store.KVLen(1 << 18)
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
	fsMem := NewFromMem(path, ms)
	if err := fsMem.Flush(blkSize); !assert.NoError(t, err) {
		panic(nil)
	}

	// create FileStore from flushed store file
	_, err := NewFromFile(path)
	if !assert.NoError(t, err) {
		panic(nil)
	}
}
