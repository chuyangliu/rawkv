package store

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

// KVLen stores the length of keys or values in bytes.
type KVLen uint64

// KVLenSize stores the size of type KVLen in bytes.
const KVLenSize KVLen = 8

// KStat stores the status of keys (i.e., Put or Deleted).
type KStat uint8

// KStatSize stores the size of type KStat in bytes.
const KStatSize KVLen = 1

// Candidate values of type KStat.
const (
	// KStatPut stores key status Put.
	KStatPut KStat = 0x00
	// KStatDel stores key status Deleted.
	KStatDel KStat = 0x01
)

// BlockSize stores minimum size in bytes of each block in FileStore.
const BlockSize KVLen = 1 << 18

// FlushBufLen stores the length of the buffer in bytes used to flush MemStore to FileStore.
const FlushBufLen KVLen = 1 << 18
