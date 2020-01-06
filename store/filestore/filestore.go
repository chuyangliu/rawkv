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
	path     string             // path to store file on disk
	blkSize  store.KVLen        // minimum block size in bytes
	blkIdx   []*blockIndexEntry // in-memory index to locate blocks in store file
	memStore *memstore.MemStore // read only, reset to nil after flushed to disk
}

// NewFromMem instantiates a FileStore from MemStore.
func NewFromMem(path string, blkSize store.KVLen, ms *memstore.MemStore) *FileStore {
	return &FileStore{
		path:     path,
		blkSize:  blkSize,
		blkIdx:   nil,
		memStore: ms,
	}
}

func (fs *FileStore) flush() error {
	file, err := os.OpenFile(fs.path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("Open file failed | path=%v | err=[%w]", fs.path, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	fs.blkIdx = make([]*blockIndexEntry, 0)
	sizeTot := store.KVLen(0)
	sizeCur := store.KVLen(0)
	sizeIdx := store.KVLen(0)

	// persist key-value data
	for i, entry := range fs.memStore.Entries() {

		if i == 0 || sizeCur >= fs.blkSize { // first block or finish one block write
			// set block length
			if cnt := len(fs.blkIdx); cnt > 0 {
				fs.blkIdx[cnt-1].len = sizeCur
			}
			// create index entry for new block
			idxEntry := &blockIndexEntry{
				key: entry.Key,
				off: sizeTot,
				len: 0, // set later
			}
			fs.blkIdx = append(fs.blkIdx, idxEntry)
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
	if cnt := len(fs.blkIdx); cnt > 0 && sizeCur > 0 {
		fs.blkIdx[cnt-1].len = sizeCur
	}

	// persist block index
	for _, entry := range fs.blkIdx {
		if err := fs.writeBlockIndexEntry(writer, entry); err != nil {
			return fmt.Errorf("Write block index entry failed | path=%v | entry=%v | err=[%w]", fs.path, *entry, err)
		}
	}

	// persist block index length
	if err := fs.writeKVLen(writer, sizeIdx); err != nil {
		return fmt.Errorf("Write block index length failed | path=%v | len=%v | err=[%w]", fs.path, sizeIdx, err)
	}

	return nil
}

func (fs *FileStore) writeKVEntry(writer *bufio.Writer, entry *store.Entry) error {

	keyLen := store.KVLen(len(entry.Key))
	if err := fs.writeKVLen(writer, keyLen); err != nil {
		return fmt.Errorf("Write key length failed | path=%v | keyLen=%v | err=[%w]", fs.path, keyLen, err)
	}

	if _, err := writer.WriteString(entry.Key.Str()); err != nil {
		return fmt.Errorf("Write key failed | path=%v | key=%v | err=[%w]", fs.path, entry.Key, err)
	}

	valLen := store.KVLen(len(entry.Val))
	if err := fs.writeKVLen(writer, valLen); err != nil {
		return fmt.Errorf("Write value length failed | path=%v | valLen=%v | err=[%w]", fs.path, valLen, err)
	}

	if _, err := writer.WriteString(entry.Val.Str()); err != nil {
		return fmt.Errorf("Write value failed | path=%v | val=%v | err=[%w]", fs.path, entry.Val, err)
	}

	if err := fs.writeKStat(writer, entry.Stat); err != nil {
		return fmt.Errorf("Write status failed | path=%v | stat=%v | err=[%w]", fs.path, entry.Stat, err)
	}

	return nil
}

func (fs *FileStore) writeBlockIndexEntry(writer *bufio.Writer, entry *blockIndexEntry) error {

	keyLen := store.KVLen(len(entry.key))
	if err := fs.writeKVLen(writer, keyLen); err != nil {
		return fmt.Errorf("Write key length failed | path=%v | keyLen=%v | err=[%w]", fs.path, keyLen, err)
	}

	if _, err := writer.WriteString(entry.key.Str()); err != nil {
		return fmt.Errorf("Write key failed | path=%v | key=%v | err=[%w]", fs.path, entry.key, err)
	}

	if err := fs.writeKVLen(writer, entry.off); err != nil {
		return fmt.Errorf("Write offset failed | path=%v | off=%v | err=[%w]", fs.path, entry.off, err)
	}

	if err := fs.writeKVLen(writer, entry.len); err != nil {
		return fmt.Errorf("Write length failed | path=%v | len=%v | err=[%w]", fs.path, entry.len, err)
	}

	return nil
}

func (fs *FileStore) writeKVLen(writer *bufio.Writer, val store.KVLen) error {
	for i := store.KVLen(0); i < store.KVLenSize; i++ { // little endian
		if err := writer.WriteByte(byte(val & 0xFF)); err != nil {
			return fmt.Errorf("Write byte failed | val=%v", val)
		}
		val >>= 8
	}
	return nil
}

func (fs *FileStore) writeKStat(writer *bufio.Writer, val store.KStat) error {
	if err := writer.WriteByte(byte(val)); err != nil {
		return fmt.Errorf("Write byte failed | val=%v", val)
	}
	return nil
}
