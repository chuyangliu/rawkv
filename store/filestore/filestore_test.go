package filestore

import (
	"testing"

	"github.com/chuyangliu/rawkv/store"
	"github.com/chuyangliu/rawkv/store/memstore"
	"github.com/stretchr/testify/assert"
)

func TestFlush(t *testing.T) {
	const max = 1000
	const blkSize = store.KVLen(1 << 18)
	const path = "./filestore.test"

	ms := memstore.New()
	for i := 0; i < max; i++ {
		ms.Put(store.Key(i), store.Value(i))
	}

	fs := NewFromMem(path, blkSize, ms)
	if err := fs.flush(); !assert.NoError(t, err) {
		panic(nil)
	}
}
