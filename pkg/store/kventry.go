package store

import (
	"unsafe"
)

// Key stores the type of keys (i.e., byte string).
type Key string

// KeyCmp compares two keys, returns -1 if k1 < k2, 1 if k1 > k2, 0 if k1 == k2.
func KeyCmp(k1, k2 interface{}) int {
	key1 := k1.(Key)
	key2 := k2.(Key)
	switch {
	case key1 < key2:
		return -1
	case key1 > key2:
		return 1
	default:
		return 0
	}
}

// Value stores the type of values (i.e., byte string).
type Value string

// KStat stores the status of keys (i.e., Put or Deleted).
type KStat uint8

// Candidate values of type KStat.
const (
	// KStatPut stores key status Put.
	KStatPut KStat = 0x00
	// KStatDel stores key status Deleted.
	KStatDel KStat = 0x01
)

// KVLen stores the length of keys or values in bytes.
type KVLen uint64

// Type sizes in bytes.
const (
	// KVLenSize stores the size of type KVLen in bytes.
	KVLenSize = KVLen(unsafe.Sizeof(KVLen(0)))
	// KStatSize stores the size of type KStat in bytes.
	KStatSize = KVLen(unsafe.Sizeof(KStat(0)))
)

// Entry stores each entry in MemStore.
type Entry struct {
	Key  Key
	Val  Value
	Stat KStat
}

// Size returns number of bytes needed to persist entry on disk.
func (e *Entry) Size() KVLen {
	return KVLenSize + KVLen(len(e.Key)) + KVLenSize + KVLen(len(e.Val)) + KStatSize
}
