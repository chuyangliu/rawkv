package raft

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"github.com/chuyangliu/rawkv/pkg/pb"
	"github.com/chuyangliu/rawkv/pkg/store"
)

// Enum values of log commands.
const (
	CmdNoOp uint32 = 0
	CmdPut  uint32 = 1
	CmdDel  uint32 = 2
)

// Log represents a raft log entry.
type Log struct {
	entry *pb.AppendEntriesReq_LogEntry
}

// newNoOpLog creates a Log with no-op command.
func newNoOpLog() *Log {
	return &Log{
		entry: &pb.AppendEntriesReq_LogEntry{
			Cmd: CmdNoOp,
		},
	}
}

// NewPutLog creates a Log with put command.
func NewPutLog(key []byte, value []byte) *Log {
	return &Log{
		entry: &pb.AppendEntriesReq_LogEntry{
			Cmd:   CmdPut,
			Key:   key,
			Value: value,
		},
	}
}

// NewDelLog creates a Log with delete command.
func NewDelLog(key []byte) *Log {
	return &Log{
		entry: &pb.AppendEntriesReq_LogEntry{
			Cmd:   CmdDel,
			Key:   key,
			Value: nil,
		},
	}
}

// newLogFromPb creates a Log from given protobuf log entry.
func newLogFromPb(entry *pb.AppendEntriesReq_LogEntry) *Log {
	return &Log{
		entry: entry,
	}
}

// newLogFromFile creates a Log from file backed by reader.
func newLogFromFile(reader io.Reader) (*Log, error) {

	var index uint64
	if err := binary.Read(reader, binary.BigEndian, &index); err != nil {
		if err == io.EOF { // Empty log file.
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
		var valueLen store.KVLen
		if err := binary.Read(reader, binary.BigEndian, &valueLen); err != nil {
			return nil, fmt.Errorf("Read log value length failed | err=[%w]", err)
		}

		value := make([]byte, valueLen)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, fmt.Errorf("Read log value failed | err=[%w]", err)
		}

		log.entry.Value = value
	}

	return log, nil
}

// String returns a string representation of the log.
func (l *Log) String() string {
	return fmt.Sprintf("[index=%v | term=%v | cmd=%v | key=%v | val=%v]",
		l.entry.GetIndex(), l.entry.GetTerm(), l.entry.GetCmd(), l.entry.GetKey(), l.entry.GetValue())
}

// Cmd returns the command associated with the log.
func (l *Log) Cmd() uint32 {
	return l.entry.GetCmd()
}

// Key returns the key associated with the log.
func (l *Log) Key() store.Key {
	return store.Key(l.entry.GetKey())
}

// Val returns the value associated with the log.
func (l *Log) Val() store.Value {
	return store.Value(l.entry.GetValue())
}

// write writes the log to writer.
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
		if err := binary.Write(writer, binary.BigEndian, store.KVLen(len(l.entry.Value))); err != nil {
			return fmt.Errorf("Write log value length failed | log=%v | err=[%w]", l, err)
		}

		if _, err := writer.Write(l.entry.Value); err != nil {
			return fmt.Errorf("Write log value failed | log=%v | err=[%w]", l, err)
		}
	}

	return nil
}

// size returns the number of bytes required to persist the log on disk.
func (l *Log) size() int64 {
	ans := unsafe.Sizeof(l.entry.Index) + unsafe.Sizeof(l.entry.Term) + unsafe.Sizeof(l.entry.Cmd) +
		unsafe.Sizeof(store.KVLen(0)) + uintptr(len(l.entry.Key))
	if l.entry.Cmd == CmdPut {
		ans += unsafe.Sizeof(store.KVLen(0)) + uintptr(len(l.entry.Value))
	}
	return int64(ans)
}
