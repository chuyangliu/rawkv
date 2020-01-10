package filestore

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/chuyangliu/rawkv/logging"
	"github.com/chuyangliu/rawkv/store"
	"github.com/chuyangliu/rawkv/store/memstore"
)

var (
	logger = logging.New(logging.LevelInfo)
)

// FileStore persists key-value data as an immutable file on disk.
type FileStore struct {
	path string             // path to store file
	mem  *memstore.MemStore // read-only, reset to nil after flushed
	idx  *blockIndex        // index to locate blocks in store file
	lock sync.RWMutex
}

// New instantiates a FileStore.
// If ms is nil, the FileStore is backed by store file on disk.
// Otherwise, ms will be used to back FileStore and can be flushed to store file.
func New(path string, ms *memstore.MemStore) *FileStore {
	return &FileStore{
		path: path,
		mem:  ms,
		idx:  nil,
	}
}

// BeginFlush flushes MemStore in background.
func (fs *FileStore) BeginFlush(blkSize store.KVLen) {
	go func() {
		if err := fs.Flush(blkSize); err != nil {
			logger.Error("Background flush failed | path=%v | err=[%w]", fs.path, err)
		}
	}()
}

// Flush persists MemStore to store file. Can be called only once.
func (fs *FileStore) Flush(blkSize store.KVLen) error {
	if fs.mem == nil {
		return fmt.Errorf("No MemStore to flush")
	}

	file, err := os.OpenFile(fs.path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("Open file failed | path=%v | err=[%w]", fs.path, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	fs.idx = newBlockIndex()
	sizeTot := store.KVLen(0)
	sizeCur := store.KVLen(0)
	sizeIdx := store.KVLen(0)

	// persist key-value data
	for i, entry := range fs.mem.Entries() {

		if i == 0 || sizeCur >= blkSize { // first block or finish one block write
			// set block length
			if !fs.idx.empty() {
				fs.idx.last().len = sizeCur
			}
			// create index entry for new block
			idxEntry := &blockIndexEntry{
				key: entry.Key,
				off: sizeTot,
				len: 0, // set later
			}
			fs.idx.add(idxEntry)
			sizeIdx += idxEntry.size()
			// reset sizes
			sizeCur = 0
		}

		if err := writeKVEntry(writer, entry); err != nil {
			return fmt.Errorf("Write kv entry failed | path=%v | entry=%v | err=[%w]", fs.path, *entry, err)
		}

		sizeCur += entry.Size()
		sizeTot += entry.Size()
	}

	// set last block length
	if !fs.idx.empty() && sizeCur > 0 {
		fs.idx.last().len = sizeCur
	}

	// persist block index
	for _, entry := range fs.idx.entries() {
		if err := writeBlockIndexEntry(writer, entry); err != nil {
			return fmt.Errorf("Write block index entry failed | path=%v | entry=%v | err=[%w]", fs.path, *entry, err)
		}
	}

	// persist block index length
	if err := writeKVLen(writer, sizeIdx); err != nil {
		return fmt.Errorf("Write block index length failed | path=%v | sizeIdx=%v | err=[%w]", fs.path, sizeIdx, err)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("Flush writer failed | path=%v | err=[%w]", fs.path, err)
	}

	fs.lock.Lock()
	fs.mem = nil
	fs.lock.Unlock()

	return nil
}

// Get returns the entry associated with the key, or nil if not exist.
func (fs *FileStore) Get(key store.Key) (*store.Entry, error) {
	fs.lock.RLock()
	ms := fs.mem
	fs.lock.RUnlock()

	if ms != nil {
		entry := ms.Get(key)
		return entry, nil
	}

	if fs.idx == nil {
		idx, err := readBlockIndex(fs.path)
		if err != nil {
			return nil, fmt.Errorf("Read block index failed | path=%v | err=[%w]", fs.path, err)
		}
		fs.idx = idx
	}

	idxEntry := fs.idx.get(key)
	if idxEntry == nil { // not exist
		return nil, nil
	}

	blk, err := readBlock(fs.path, idxEntry)
	if err != nil {
		return nil, fmt.Errorf("Read block failed | path=%v | err=[%w]", fs.path, err)
	}

	entry := blk.get(key)
	return entry, nil
}

func readBlockIndex(path string) (*blockIndex, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Open file failed | path=%v | err=[%w]", path, err)
	}
	defer file.Close()

	// seek index length
	if _, err := file.Seek(-int64(store.KVLenSize), os.SEEK_END); err != nil {
		return nil, fmt.Errorf("Seek index length failed | path=%v | err=[%w]", path, err)
	}

	// read index length
	idxLen, err := readKVLen(file)
	if err != nil {
		return nil, fmt.Errorf("Read index length failed | path=%v | err=[%w]", path, err)
	}

	// seek index entry
	if _, err := file.Seek(-int64(store.KVLenSize+idxLen), os.SEEK_END); err != nil {
		return nil, fmt.Errorf("Seek index entry failed | path=%v | idxLen=%v | err=[%w]", path, idxLen, err)
	}

	// read index bytes
	raw := make([]byte, idxLen)
	if _, err := io.ReadFull(file, raw); err != nil {
		return nil, fmt.Errorf("Read index bytes failed | path=%v | idxLen=%v | err=[%w]", path, idxLen, err)
	}

	reader := bytes.NewReader(raw)
	idx := newBlockIndex()
	size := store.KVLen(0)

	// read index entries
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

func readBlock(path string, idxEntry *blockIndexEntry) (*fileBlock, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Open file failed | path=%v | err=[%w]", path, err)
	}
	defer file.Close()

	// seek block start
	if _, err := file.Seek(int64(idxEntry.off), os.SEEK_SET); err != nil {
		return nil, fmt.Errorf("Seek block start failed | path=%v | err=[%w]", path, err)
	}

	// read block bytes
	raw := make([]byte, idxEntry.len)
	if _, err := io.ReadFull(file, raw); err != nil {
		return nil, fmt.Errorf("Read block bytes failed | path=%v | err=[%w]", path, err)
	}

	reader := bytes.NewReader(raw)
	blk := newBlock()
	size := store.KVLen(0)

	// read kv entries
	for size < idxEntry.len {
		entry, err := readKVEntry(reader)
		if err != nil {
			return nil, fmt.Errorf("Read kv entry failed | path=%v | size=%v | blkLen=%v | err=[%w]",
				path, size, idxEntry.len, err)
		}
		blk.add(entry)
		size += entry.Size()
	}

	return blk, nil
}

func writeKVEntry(writer *bufio.Writer, entry *store.Entry) error {

	keyLen := store.KVLen(len(entry.Key))
	if err := writeKVLen(writer, keyLen); err != nil {
		return fmt.Errorf("Write key length failed | keyLen=%v | err=[%w]", keyLen, err)
	}

	if _, err := writer.WriteString(string(entry.Key)); err != nil {
		return fmt.Errorf("Write key failed | key=%v | err=[%w]", entry.Key, err)
	}

	valLen := store.KVLen(len(entry.Val))
	if err := writeKVLen(writer, valLen); err != nil {
		return fmt.Errorf("Write value length failed | valLen=%v | err=[%w]", valLen, err)
	}

	if _, err := writer.WriteString(string(entry.Val)); err != nil {
		return fmt.Errorf("Write value failed | val=%v | err=[%w]", entry.Val, err)
	}

	if err := writeKStat(writer, entry.Stat); err != nil {
		return fmt.Errorf("Write status failed | stat=%v | err=[%w]", entry.Stat, err)

	}

	return nil
}

func readKVEntry(reader io.Reader) (*store.Entry, error) {

	keyLen, err := readKVLen(reader)
	if err != nil {
		return nil, fmt.Errorf("Read key length failed | err=[%w]", err)
	}

	key, err := readKey(reader, keyLen)
	if err != nil {
		return nil, fmt.Errorf("Read key failed | err=[%w]", err)
	}

	valLen, err := readKVLen(reader)
	if err != nil {
		return nil, fmt.Errorf("Read value length failed | err=[%w]", err)
	}

	val, err := readValue(reader, valLen)
	if err != nil {
		return nil, fmt.Errorf("Read value failed | err=[%w]", err)
	}

	stat, err := readKStat(reader)
	if err != nil {
		return nil, fmt.Errorf("Read status failed | err=[%w]", err)
	}

	return &store.Entry{
		Key:  key,
		Val:  val,
		Stat: stat,
	}, nil
}

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

func readKey(reader io.Reader, keyLen store.KVLen) (store.Key, error) {
	raw := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, raw); err != nil {
		return "", fmt.Errorf("Read full failed | err=[%w]", err)
	}
	return store.Key(raw), nil
}

func readValue(reader io.Reader, valLen store.KVLen) (store.Value, error) {
	raw := make([]byte, valLen)
	if _, err := io.ReadFull(reader, raw); err != nil {
		return "", fmt.Errorf("Read full failed | err=[%w]", err)
	}
	return store.Value(raw), nil
}

func writeKStat(writer *bufio.Writer, val store.KStat) error {
	if err := writer.WriteByte(byte(val)); err != nil {
		return fmt.Errorf("Write byte failed | val=%v | err=[%w]", val, err)
	}
	return nil
}

func readKStat(reader io.Reader) (store.KStat, error) {
	raw := make([]byte, store.KStatSize)
	if _, err := io.ReadFull(reader, raw); err != nil {
		return 0, fmt.Errorf("Read full failed | err=[%w]", err)
	}
	val := store.KStat(0)
	for i, b := range raw {
		val |= store.KStat(b) << (8 * i)
	}
	return val, nil
}

func writeKVLen(writer *bufio.Writer, val store.KVLen) error {
	for i := store.KVLen(0); i < store.KVLenSize; i++ {
		if err := writer.WriteByte(byte(val & 0xFF)); err != nil {
			return fmt.Errorf("Write byte failed | val=%v | err=[%w]", val, err)
		}
		val >>= 8
	}
	return nil
}

func readKVLen(reader io.Reader) (store.KVLen, error) {
	raw := make([]byte, store.KVLenSize)
	if _, err := io.ReadFull(reader, raw); err != nil {
		return 0, fmt.Errorf("Read full failed | err=[%w]", err)
	}
	val := store.KVLen(0)
	for i, b := range raw {
		val |= store.KVLen(b) << (8 * i)
	}
	return val, nil
}
