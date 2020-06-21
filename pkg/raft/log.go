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

type raftLog struct {
	entry *pb.AppendEntriesReq_LogEntry
}

func newRaftLog(entry *pb.AppendEntriesReq_LogEntry) *raftLog {
	return &raftLog{
		entry: entry,
	}
}

func newRaftLogFromFile(reader io.Reader) (*raftLog, error) {

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

	log := &raftLog{
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

func (rl *raftLog) String() string {
	return fmt.Sprintf("[index=%v | term=%v | cmd=%v | key=%v | val=%v]",
		rl.entry.GetIndex(), rl.entry.GetTerm(), rl.entry.GetCmd(), rl.entry.GetKey(), rl.entry.GetVal())
}

func (rl *raftLog) write(writer io.Writer) error {

	if err := binary.Write(writer, binary.BigEndian, rl.entry.Index); err != nil {
		return fmt.Errorf("Write log index failed | log=%v | err=[%w]", rl, err)
	}

	if err := binary.Write(writer, binary.BigEndian, rl.entry.Term); err != nil {
		return fmt.Errorf("Write log term failed | log=%v | err=[%w]", rl, err)
	}

	if err := binary.Write(writer, binary.BigEndian, rl.entry.Cmd); err != nil {
		return fmt.Errorf("Write log command failed | log=%v | err=[%w]", rl, err)
	}

	if err := binary.Write(writer, binary.BigEndian, store.KVLen(len(rl.entry.Key))); err != nil {
		return fmt.Errorf("Write log key length failed | log=%v | err=[%w]", rl, err)
	}

	if _, err := writer.Write(rl.entry.Key); err != nil {
		return fmt.Errorf("Write log key failed | log=%v | err=[%w]", rl, err)
	}

	if rl.entry.Cmd == CmdPut {
		if err := binary.Write(writer, binary.BigEndian, store.KVLen(len(rl.entry.Val))); err != nil {
			return fmt.Errorf("Write log value length failed | log=%v | err=[%w]", rl, err)
		}

		if _, err := writer.Write(rl.entry.Val); err != nil {
			return fmt.Errorf("Write log value failed | log=%v | err=[%w]", rl, err)
		}
	}

	return nil
}

func (rl *raftLog) size() int64 {
	ans := unsafe.Sizeof(rl.entry.Index) + unsafe.Sizeof(rl.entry.Term) + unsafe.Sizeof(rl.entry.Cmd) +
		unsafe.Sizeof(store.KVLen(0)) + uintptr(len(rl.entry.Key))
	if rl.entry.Cmd == CmdPut {
		ans += unsafe.Sizeof(store.KVLen(0)) + uintptr(len(rl.entry.Val))
	}
	return int64(ans)
}
