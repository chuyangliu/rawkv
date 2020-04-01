// Package store defines types of keys and values, and provides helper functions.
package store

import (
	"unsafe"
)

// Key represents the type of keys (i.e., byte string).
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

// Value represents the type of values (i.e., byte string).
type Value string

// Status represents the status of key-value pairs (i.e., Put or Deleted).
type Status uint8

// Enum values of type Status.
const (
	// StatusPut store status Put.
	StatusPut Status = 0x00
	// StatusDel store status Deleted.
	StatusDel Status = 0x01
)

// KVLen represents the length of keys or values in bytes.
type KVLen uint64

// Type sizes in bytes.
const (
	// KVLenSize stores the size of type KVLen in bytes.
	KVLenSize = KVLen(unsafe.Sizeof(KVLen(0)))
	// StatusSize stores the size of type Status in bytes.
	StatusSize = KVLen(unsafe.Sizeof(Status(0)))
)

// Entry represents each entry in an MemStore.
type Entry struct {
	Key
	Value
	Status
}

// Size returns number of bytes needed to persist an Entry on disk.
func (e *Entry) Size() KVLen {
	return KVLenSize + KVLen(len(e.Key)) + KVLenSize + KVLen(len(e.Value)) + StatusSize
}
