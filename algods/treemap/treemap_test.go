package treemap

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	data := []struct {
		key string
		val interface{}
	}{
		{"a", "100"},
		{"ak10", true},
		{"b", 9999},
		{"bz", 106.12},
		{"caz", "108"},
		{"cbduk", -110},
		{"dlmpq", "112"},
		{"ef", -114.912},
		{"klm", "116"},
		{"kpp1", false},
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(data), func(i, j int) { data[i], data[j] = data[j], data[i] })

	tm := New()
	for _, entry := range data {
		tm.Put(entry.key, entry.val)
	}

	sort.Slice(data, func(i, j int) bool { return data[i].key < data[j].key })

	for i, rawKey := range tm.Keys() {
		rawVal, _ := tm.Get(rawKey)

		if !assert.Equal(t, rawKey, data[i].key) {
			panic(nil)
		}
		if !assert.Equal(t, rawVal, data[i].val) {
			panic(nil)
		}
	}
}
