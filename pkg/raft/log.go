package raft

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"github.com/chuyangliu/rawkv/pkg/pb"
	"github.com/chuyangliu/rawkv/pkg/store"
)

const (
	// CmdPut stores enum value of put operation.
	CmdPut uint32 = 0
	// CmdDel stores enum value of delete operation.
	CmdDel uint32 = 1
)

// Log represents raft replicated log entry.
type Log struct {
	entry *pb.AppendEntriesReq_LogEntry
}

// NewPutLog creates a new raft log to store put operation.
func NewPutLog(key []byte, val []byte) *Log {
	return &Log{
		entry: &pb.AppendEntriesReq_LogEntry{
			Cmd: CmdPut,
			Key: key,
			Val: val,
		},
	}
}

// NewDelLog creates a new raft log to store delete operation.
func NewDelLog(key []byte) *Log {
	return &Log{
		entry: &pb.AppendEntriesReq_LogEntry{
			Cmd: CmdDel,
			Key: key,
			Val: nil,
		},
	}
}

func newLogFromPb(entry *pb.AppendEntriesReq_LogEntry) *Log {
	return &Log{
		entry: entry,
	}
}

func newLogFromFile(reader io.Reader) (*Log, error) {

	var index uint64
	if err := binary.Read(reader, binary.BigEndian, &index); err != nil {
		if err == io.EOF { // empty log file
			return nil, nil
		}
		return nil, fmt.Errorf("Read log index failed | err=[%w]", err)
	}

	var term uint64
	if err := binary.Read(reader, binary.BigEndian, &term); err != nil {
		return nil, fmt.Errorf("Read log term failed | err=[%w]", err)
	}

	var cmd uint32
	if err := binary.Read(reader, binary.BigEndian, &cmd); err != nil {
		return nil, fmt.Errorf("Read log command failed | err=[%w]", err)
	}

	var keyLen store.KVLen
	if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
		return nil, fmt.Errorf("Read log key length failed | err=[%w]", err)
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, fmt.Errorf("Read log key failed | err=[%w]", err)
	}

	log := &Log{
		entry: &pb.AppendEntriesReq_LogEntry{
			Index: index,
			Term:  term,
			Cmd:   cmd,
			Key:   key,
		},
	}

	if cmd == CmdPut {
		var valLen store.KVLen
		if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
			return nil, fmt.Errorf("Read log value length failed | err=[%w]", err)
		}

		val := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, val); err != nil {
			return nil, fmt.Errorf("Read log value failed | err=[%w]", err)
		}

		log.entry.Val = val
	}

	return log, nil
}

func (l *Log) String() string {
	return fmt.Sprintf("[index=%v | term=%v | cmd=%v | key=%v | val=%v]",
		l.entry.GetIndex(), l.entry.GetTerm(), l.entry.GetCmd(), l.entry.GetKey(), l.entry.GetVal())
}

// Cmd returns the command stored in the log.
func (l *Log) Cmd() uint32 {
	return l.entry.GetCmd()
}

// Key returns the key stored in the log.
func (l *Log) Key() store.Key {
	return store.Key(l.entry.GetKey())
}

// Val returns the value stored in the log.
func (l *Log) Val() store.Value {
	return store.Value(l.entry.GetVal())
}

func (l *Log) write(writer io.Writer) error {

	if err := binary.Write(writer, binary.BigEndian, l.entry.Index); err != nil {
		return fmt.Errorf("Write log index failed | log=%v | err=[%w]", l, err)
	}

	if err := binary.Write(writer, binary.BigEndian, l.entry.Term); err != nil {
		return fmt.Errorf("Write log term failed | log=%v | err=[%w]", l, err)
	}

	if err := binary.Write(writer, binary.BigEndian, l.entry.Cmd); err != nil {
		return fmt.Errorf("Write log command failed | log=%v | err=[%w]", l, err)
	}

	if err := binary.Write(writer, binary.BigEndian, store.KVLen(len(l.entry.Key))); err != nil {
		return fmt.Errorf("Write log key length failed | log=%v | err=[%w]", l, err)
	}

	if _, err := writer.Write(l.entry.Key); err != nil {
		return fmt.Errorf("Write log key failed | log=%v | err=[%w]", l, err)
	}

	if l.entry.Cmd == CmdPut {
		if err := binary.Write(writer, binary.BigEndian, store.KVLen(len(l.entry.Val))); err != nil {
			return fmt.Errorf("Write log value length failed | log=%v | err=[%w]", l, err)
		}

		if _, err := writer.Write(l.entry.Val); err != nil {
			return fmt.Errorf("Write log value failed | log=%v | err=[%w]", l, err)
		}
	}

	return nil
}

func (l *Log) size() int64 {
	ans := unsafe.Sizeof(l.entry.Index) + unsafe.Sizeof(l.entry.Term) + unsafe.Sizeof(l.entry.Cmd) +
		unsafe.Sizeof(store.KVLen(0)) + uintptr(len(l.entry.Key))
	if l.entry.Cmd == CmdPut {
		ans += unsafe.Sizeof(store.KVLen(0)) + uintptr(len(l.entry.Val))
	}
	return int64(ans)
}
