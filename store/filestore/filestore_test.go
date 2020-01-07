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

	// flush MemStore
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
