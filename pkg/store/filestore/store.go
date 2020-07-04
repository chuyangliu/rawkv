// Package filestore implements the on-disk storage layer of an LSM tree.
package filestore

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/chuyangliu/rawkv/pkg/logging"
	"github.com/chuyangliu/rawkv/pkg/store"
	"github.com/chuyangliu/rawkv/pkg/store/memstore"
)

// Store implements the on-disk storage layer of an LSM tree.
type Store struct {
	logger *logging.Logger
	lock   sync.RWMutex    // Reader/writer lock to protect concurrent accesses.
	path   string          // Path to store file.
	mem    *memstore.Store // Read-only MemStore to flush. Reset to nil after flushed.
	index  *blockIndex     // Index to locate blocks in store file.
}

// New creates a FileStore with given logging level and path to store file.
// If mem is nil, the FileStore is backed by store file.
// Otherwise, mem will be used to create a new store file.
func New(level int, path string, mem *memstore.Store) (*Store, error) {

	s := &Store{
		logger: logging.New(level),
		path:   path,
		mem:    mem,
		index:  nil,
	}

	if mem == nil {
		index, err := readBlockIndex(path)
		if err != nil {
			return nil, fmt.Errorf("Read block index failed | path=%v | err=[%w]", path, err)
		}
		s.index = index
	}

	return s, nil
}

// Get returns the entry associated with key, or nil if key is not found.
func (s *Store) Get(key store.Key) (*store.Entry, error) {
	s.lock.RLock()
	mem := s.mem
	s.lock.RUnlock()

	if mem != nil {
		entry := mem.Get(key)
		return entry, nil
	}

	indexEntry := s.index.get(key)
	if indexEntry == nil { // Key not found.
		return nil, nil
	}

	block, err := readBlock(s.path, indexEntry)
	if err != nil {
		return nil, fmt.Errorf("Read block failed | path=%v | err=[%w]", s.path, err)
	}

	entry := block.get(key)
	return entry, nil
}

// BeginFlush flushes MemStore to store file in a separate goroutine.
func (s *Store) BeginFlush(blockSize store.KVLen) {
	go func() {
		if err := s.Flush(blockSize); err != nil {
			s.logger.Error("Background flush failed | path=%v | err=[%v]", s.path, err)
		}
	}()
}

// Flush flushes MemStore to store file. The method can be called only once.
func (s *Store) Flush(blockSize store.KVLen) error {

	if s.mem == nil {
		return fmt.Errorf("No MemStore to flush")
	}

	file, err := os.OpenFile(s.path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("Open file failed | path=%v | err=[%w]", s.path, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	s.index = newBlockIndex()
	sizeTot := store.KVLen(0)
	sizeCur := store.KVLen(0)
	sizeIndex := store.KVLen(0)

	// Flush key-value data.
	for i, entry := range s.mem.Entries() {

		if i == 0 || sizeCur >= blockSize { // First block or finish one block write.
			// Set block length.
			if !s.index.empty() {
				s.index.last().len = sizeCur
			}
			// Create index entry for new block.
			idxEntry := &blockIndexEntry{
				key: entry.Key,
				off: sizeTot,
				len: 0, // Set later.
			}
			s.index.add(idxEntry)
			sizeIndex += idxEntry.size()
			// Reset sizes.
			sizeCur = 0
		}

		if err := writeKVEntry(writer, entry); err != nil {
			return fmt.Errorf("Write kv entry failed | path=%v | entry=%v | err=[%w]", s.path, *entry, err)
		}

		sizeCur += entry.Size()
		sizeTot += entry.Size()
	}

	// Set last block length.
	if !s.index.empty() && sizeCur > 0 {
		s.index.last().len = sizeCur
	}

	// Flush block index.
	for _, entry := range s.index.entries {
		if err := writeBlockIndexEntry(writer, entry); err != nil {
			return fmt.Errorf("Write block index entry failed | path=%v | entry=%v | err=[%w]", s.path, *entry, err)
		}
	}

	// Flush block index length.
	if err := writeKVLen(writer, sizeIndex); err != nil {
		return fmt.Errorf("Write block index length failed | path=%v | sizeIndex=%v | err=[%w]", s.path, sizeIndex, err)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("Flush writer failed | path=%v | err=[%w]", s.path, err)
	}

	s.lock.Lock()
	s.mem = nil
	s.lock.Unlock()

	return nil
}

// readBlockIndex reads the block index of the store file located at path.
func readBlockIndex(path string) (*blockIndex, error) {

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Open file failed | path=%v | err=[%w]", path, err)
	}
	defer file.Close()

	// Seek index length.
	if _, err := file.Seek(-int64(store.KVLenSize), os.SEEK_END); err != nil {
		return nil, fmt.Errorf("Seek index length failed | path=%v | err=[%w]", path, err)
	}

	// Read index length.
	idxLen, err := readKVLen(file)
	if err != nil {
		return nil, fmt.Errorf("Read index length failed | path=%v | err=[%w]", path, err)
	}

	// Seek index entry.
	if _, err := file.Seek(-int64(store.KVLenSize+idxLen), os.SEEK_END); err != nil {
		return nil, fmt.Errorf("Seek index entry failed | path=%v | idxLen=%v | err=[%w]", path, idxLen, err)
	}

	// Read index bytes.
	raw := make([]byte, idxLen)
	if _, err := io.ReadFull(file, raw); err != nil {
		return nil, fmt.Errorf("Read index bytes failed | path=%v | idxLen=%v | err=[%w]", path, idxLen, err)
	}

	reader := bytes.NewReader(raw)
	idx := newBlockIndex()
	size := store.KVLen(0)

	// Read index entries.
	for size < idxLen {
		entry, err := readBlockIndexEntry(reader)
		if err != nil {
			return nil, fmt.Errorf("Read index entry failed | path=%v | size=%v | idxLen=%v | err=[%w]",
				path, size, idxLen, err)
		}
		idx.add(entry)
		size += entry.size()
	}

	return idx, nil
}

// readBlock reads a block from store file located at path.
// The read offset and length are specified in indexEntry.
func readBlock(path string, indexEntry *blockIndexEntry) (*fileBlock, error) {

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Open file failed | path=%v | err=[%w]", path, err)
	}
	defer file.Close()

	// Seek block start.
	if _, err := file.Seek(int64(indexEntry.off), os.SEEK_SET); err != nil {
		return nil, fmt.Errorf("Seek block start failed | path=%v | err=[%w]", path, err)
	}

	// Read block bytes.
	raw := make([]byte, indexEntry.len)
	if _, err := io.ReadFull(file, raw); err != nil {
		return nil, fmt.Errorf("Read block bytes failed | path=%v | err=[%w]", path, err)
	}

	reader := bytes.NewReader(raw)
	block := newBlock()
	size := store.KVLen(0)

	// Read key-value entries.
	for size < indexEntry.len {
		entry, err := readKVEntry(reader)
		if err != nil {
			return nil, fmt.Errorf("Read kv entry failed | path=%v | size=%v | blockLen=%v | err=[%w]",
				path, size, indexEntry.len, err)
		}
		block.add(entry)
		size += entry.Size()
	}

	return block, nil
}

// writeKVEntry writes a key-value entry to writer.
func writeKVEntry(writer *bufio.Writer, entry *store.Entry) error {

	keyLen := store.KVLen(len(entry.Key))
	if err := writeKVLen(writer, keyLen); err != nil {
		return fmt.Errorf("Write key length failed | keyLen=%v | err=[%w]", keyLen, err)
	}

	if _, err := writer.WriteString(string(entry.Key)); err != nil {
		return fmt.Errorf("Write key failed | key=%v | err=[%w]", entry.Key, err)
	}

	valueLen := store.KVLen(len(entry.Value))
	if err := writeKVLen(writer, valueLen); err != nil {
		return fmt.Errorf("Write value length failed | valueLen=%v | err=[%w]", valueLen, err)
	}

	if _, err := writer.WriteString(string(entry.Value)); err != nil {
		return fmt.Errorf("Write value failed | val=%v | err=[%w]", entry.Value, err)
	}

	if err := writeStatus(writer, entry.Status); err != nil {
		return fmt.Errorf("Write status failed | stat=%v | err=[%w]", entry.Status, err)

	}

	return nil
}

// readKVEntry reads a key-value entry from reader.
func readKVEntry(reader io.Reader) (*store.Entry, error) {

	keyLen, err := readKVLen(reader)
	if err != nil {
		return nil, fmt.Errorf("Read key length failed | err=[%w]", err)
	}

	key, err := readKey(reader, keyLen)
	if err != nil {
		return nil, fmt.Errorf("Read key failed | err=[%w]", err)
	}

	valueLen, err := readKVLen(reader)
	if err != nil {
		return nil, fmt.Errorf("Read value length failed | err=[%w]", err)
	}

	value, err := readValue(reader, valueLen)
	if err != nil {
		return nil, fmt.Errorf("Read value failed | err=[%w]", err)
	}

	status, err := readStatus(reader)
	if err != nil {
		return nil, fmt.Errorf("Read status failed | err=[%w]", err)
	}

	return &store.Entry{
		Key:    key,
		Value:  value,
		Status: status,
	}, nil
}

// writeBlockIndexEntry writes a block index entry to writer.
func writeBlockIndexEntry(writer *bufio.Writer, entry *blockIndexEntry) error {

	keyLen := store.KVLen(len(entry.key))
	if err := writeKVLen(writer, keyLen); err != nil {
		return fmt.Errorf("Write key length failed | keyLen=%v | err=[%w]", keyLen, err)
	}

	if _, err := writer.WriteString(string(entry.key)); err != nil {
		return fmt.Errorf("Write key failed | key=%v | err=[%w]", entry.key, err)
	}

	if err := writeKVLen(writer, entry.off); err != nil {
		return fmt.Errorf("Write offset failed | off=%v | err=[%w]", entry.off, err)
	}

	if err := writeKVLen(writer, entry.len); err != nil {
		return fmt.Errorf("Write length failed | len=%v | err=[%w]", entry.len, err)
	}

	return nil
}

// readBlockIndexEntry reads a block index entry from reader.
func readBlockIndexEntry(reader io.Reader) (*blockIndexEntry, error) {

	keyLen, err := readKVLen(reader)
	if err != nil {
		return nil, fmt.Errorf("Read key length failed | err=[%w]", err)
	}

	key, err := readKey(reader, keyLen)
	if err != nil {
		return nil, fmt.Errorf("Read key failed | keyLen=%v | err=[%w]", keyLen, err)
	}

	off, err := readKVLen(reader)
	if err != nil {
		return nil, fmt.Errorf("Read offset failed | err=[%w]", err)
	}

	len, err := readKVLen(reader)
	if err != nil {
		return nil, fmt.Errorf("Read length failed | err=[%w]", err)
	}

	return &blockIndexEntry{
		key: key,
		off: off,
		len: len,
	}, nil
}

// readKey reads a key of length len from reader.
func readKey(reader io.Reader, len store.KVLen) (store.Key, error) {
	raw := make([]byte, len)
	if _, err := io.ReadFull(reader, raw); err != nil {
		return "", fmt.Errorf("Read full failed | err=[%w]", err)
	}
	return store.Key(raw), nil
}

// readValue reads a value of length len from reader.
func readValue(reader io.Reader, len store.KVLen) (store.Value, error) {
	raw := make([]byte, len)
	if _, err := io.ReadFull(reader, raw); err != nil {
		return "", fmt.Errorf("Read full failed | err=[%w]", err)
	}
	return store.Value(raw), nil
}

// writeStatus writes a status value to writer.
func writeStatus(writer *bufio.Writer, status store.Status) error {
	if err := writer.WriteByte(byte(status)); err != nil {
		return fmt.Errorf("Write byte failed | status=%v | err=[%w]", status, err)
	}
	return nil
}

// readStatus reads a status value from reader.
func readStatus(reader io.Reader) (store.Status, error) {
	raw := make([]byte, store.StatusSize)
	if _, err := io.ReadFull(reader, raw); err != nil {
		return 0, fmt.Errorf("Read full failed | err=[%w]", err)
	}
	status := store.Status(0)
	for i, b := range raw {
		status |= store.Status(b) << (8 * i)
	}
	return status, nil
}

// writeKVLen writes a length value len to writer.
func writeKVLen(writer *bufio.Writer, len store.KVLen) error {
	for i := store.KVLen(0); i < store.KVLenSize; i++ {
		if err := writer.WriteByte(byte(len & 0xFF)); err != nil {
			return fmt.Errorf("Write byte failed | len=%v | err=[%w]", len, err)
		}
		len >>= 8
	}
	return nil
}

// readKVLen reads a length value from reader.
func readKVLen(reader io.Reader) (store.KVLen, error) {
	raw := make([]byte, store.KVLenSize)
	if _, err := io.ReadFull(reader, raw); err != nil {
		return 0, fmt.Errorf("Read full failed | err=[%w]", err)
	}
	len := store.KVLen(0)
	for i, b := range raw {
		len |= store.KVLen(b) << (8 * i)
	}
	return len, nil
}
