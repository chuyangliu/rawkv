package filestore

import (
	"bufio"
	"fmt"
	"os"

	"github.com/chuyangliu/rawkv/store"
	"github.com/chuyangliu/rawkv/store/memstore"
)

// FileStore persists key-value data as an immutable file on disk.
type FileStore struct {
	path     string             // path to store file
	memStore *memstore.MemStore // read only, reset to nil after flushed
	blkIdx   *blockIndex        // in-memory index to locate blocks in store file
}

// NewFromMem instantiates a FileStore from MemStore.
func NewFromMem(path string, ms *memstore.MemStore) *FileStore {
	return &FileStore{
		path:     path,
		memStore: ms,
		blkIdx:   nil,
	}
}

// NewFromFile instantiates a FileStore from store file.
func NewFromFile(path string) (*FileStore, error) {
	fs := &FileStore{
		path:     path,
		memStore: nil,
		blkIdx:   nil,
	}
	if err := fs.readBlockIndex(); err != nil {
		return nil, fmt.Errorf("Read block index failed | path=%v | err=[%w]", path, err)
	}
	return fs, nil
}

// Flush persists MemStore to store file.
func (fs *FileStore) Flush(blkSize store.KVLen) error {
	file, err := os.OpenFile(fs.path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("Open file failed | path=%v | err=[%w]", fs.path, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	fs.blkIdx = newBlockIndex()
	sizeTot := store.KVLen(0)
	sizeCur := store.KVLen(0)
	sizeIdx := store.KVLen(0)

	// persist key-value data
	for i, entry := range fs.memStore.Entries() {

		if i == 0 || sizeCur >= blkSize { // first block or finish one block write
			// set block length
			if !fs.blkIdx.empty() {
				fs.blkIdx.last().len = sizeCur
			}
			// create index entry for new block
			idxEntry := &blockIndexEntry{
				key: entry.Key,
				off: sizeTot,
				len: 0, // set later
			}
			fs.blkIdx.add(idxEntry)
			sizeIdx += idxEntry.size()
			// reset sizes
			sizeCur = 0
		}

		if err := fs.writeKVEntry(writer, entry); err != nil {
			return fmt.Errorf("Write kv entry failed | path=%v | entry=%v | err=[%w]", fs.path, *entry, err)
		}

		sizeCur += entry.Size()
		sizeTot += entry.Size()
	}

	// set last block length
	if !fs.blkIdx.empty() && sizeCur > 0 {
		fs.blkIdx.last().len = sizeCur
	}

	// persist block index
	for _, entry := range fs.blkIdx.entries() {
		if err := fs.writeBlockIndexEntry(writer, entry); err != nil {
			return fmt.Errorf("Write block index entry failed | path=%v | entry=%v | err=[%w]", fs.path, *entry, err)
		}
	}

	// persist block index length
	if err := fs.writeKVLen(writer, sizeIdx); err != nil {
		return fmt.Errorf("Write block index length failed | path=%v | sizeIdx=%v | err=[%w]", fs.path, sizeIdx, err)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("Flush writer failed | path=%v | err=[%w]", fs.path, err)
	}

	fs.memStore = nil
	return nil
}

func (fs *FileStore) readBlockIndex() error {
	file, err := os.Open(fs.path)
	if err != nil {
		return fmt.Errorf("Open file failed | path=%v | err=[%w]", fs.path, err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// seek index length
	if _, err := file.Seek(-int64(store.KVLenSize), os.SEEK_END); err != nil {
		return fmt.Errorf("Seek index length failed | path=%v | err=[%w]", fs.path, err)
	}

	// read index length
	idxLen, err := fs.readKVLen(reader)
	if err != nil {
		return fmt.Errorf("Read index length failed | path=%v | err=[%w]", fs.path, err)
	}

	// seek index entry
	if _, err := file.Seek(-int64(store.KVLenSize+idxLen), os.SEEK_END); err != nil {
		return fmt.Errorf("Seek index entry failed | path=%v | idxLen=%v | err=[%w]", fs.path, idxLen, err)
	}

	fs.blkIdx = newBlockIndex()
	size := store.KVLen(0)

	// read index entries
	for size < idxLen {
		entry, err := fs.readBlockIndexEntry(reader)
		if err != nil {
			return fmt.Errorf("Read entry failed | path=%v | size=%v | idxLen=%v | err=[%w]",
				fs.path, size, idxLen, err)
		}
		fs.blkIdx.add(entry)
		size += entry.size()
	}

	return nil
}

func (fs *FileStore) writeKVEntry(writer *bufio.Writer, entry *store.Entry) error {

	keyLen := store.KVLen(len(entry.Key))
	if err := fs.writeKVLen(writer, keyLen); err != nil {
		return fmt.Errorf("Write key length failed | keyLen=%v | err=[%w]", keyLen, err)
	}

	if _, err := writer.WriteString(string(entry.Key)); err != nil {
		return fmt.Errorf("Write key failed | key=%v | err=[%w]", entry.Key, err)
	}

	valLen := store.KVLen(len(entry.Val))
	if err := fs.writeKVLen(writer, valLen); err != nil {
		return fmt.Errorf("Write value length failed | valLen=%v | err=[%w]", valLen, err)
	}

	if _, err := writer.WriteString(string(entry.Val)); err != nil {
		return fmt.Errorf("Write value failed | val=%v | err=[%w]", entry.Val, err)
	}

	if err := fs.writeKStat(writer, entry.Stat); err != nil {
		return fmt.Errorf("Write status failed | stat=%v | err=[%w]", entry.Stat, err)

	}

	return nil
}

func (fs *FileStore) writeBlockIndexEntry(writer *bufio.Writer, entry *blockIndexEntry) error {

	keyLen := store.KVLen(len(entry.key))
	if err := fs.writeKVLen(writer, keyLen); err != nil {
		return fmt.Errorf("Write key length failed | keyLen=%v | err=[%w]", keyLen, err)
	}

	if _, err := writer.WriteString(string(entry.key)); err != nil {
		return fmt.Errorf("Write key failed | key=%v | err=[%w]", entry.key, err)
	}

	if err := fs.writeKVLen(writer, entry.off); err != nil {
		return fmt.Errorf("Write offset failed | off=%v | err=[%w]", entry.off, err)
	}

	if err := fs.writeKVLen(writer, entry.len); err != nil {
		return fmt.Errorf("Write length failed | len=%v | err=[%w]", entry.len, err)
	}

	return nil
}

func (fs *FileStore) readBlockIndexEntry(reader *bufio.Reader) (*blockIndexEntry, error) {

	keyLen, err := fs.readKVLen(reader)
	if err != nil {
		return nil, fmt.Errorf("Read key length failed | err=[%w]", err)
	}

	key, err := fs.readKey(reader, keyLen)
	if err != nil {
		return nil, fmt.Errorf("Read key failed | keyLen=%v | err=[%w]", keyLen, err)
	}

	off, err := fs.readKVLen(reader)
	if err != nil {
		return nil, fmt.Errorf("Read offset failed | err=[%w]", err)
	}

	len, err := fs.readKVLen(reader)
	if err != nil {
		return nil, fmt.Errorf("Read length failed | err=[%w]", err)
	}

	return &blockIndexEntry{
		key: key,
		off: off,
		len: len,
	}, nil
}

func (fs *FileStore) writeKVLen(writer *bufio.Writer, val store.KVLen) error {
	for i := store.KVLen(0); i < store.KVLenSize; i++ {
		if err := writer.WriteByte(byte(val & 0xFF)); err != nil {
			return fmt.Errorf("Write byte failed | val=%v | err=[%w]", val, err)
		}
		val >>= 8
	}
	return nil
}

func (fs *FileStore) readKVLen(reader *bufio.Reader) (store.KVLen, error) {
	val := store.KVLen(0)
	for i := store.KVLen(0); i < store.KVLenSize; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, fmt.Errorf("Read byte failed | val=%v | err=[%w]", val, err)
		}
		val |= store.KVLen(b) << (8 * i)
	}
	return val, nil
}

func (fs *FileStore) writeKStat(writer *bufio.Writer, val store.KStat) error {
	if err := writer.WriteByte(byte(val)); err != nil {
		return fmt.Errorf("Write byte failed | val=%v | err=[%w]", val, err)
	}
	return nil
}

func (fs *FileStore) readKey(reader *bufio.Reader, keyLen store.KVLen) (store.Key, error) {
	raw := make([]byte, keyLen)
	for i := store.KVLen(0); i < keyLen; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			return "", fmt.Errorf("Read byte failed | err=[%w]", err)
		}
		raw[i] = b
	}
	return store.Key(raw), nil
}
