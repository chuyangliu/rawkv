package raft

import (
	"errors"
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

	index, err := readIndex(reader)
	if err != nil {
		if errors.Unwrap(err) == io.EOF {
			return nil, nil
		}
		return nil, fmt.Errorf("Read log index failed | err=[%w]", err)
	}

	term, err := readTerm(reader)
	if err != nil {
		return nil, fmt.Errorf("Read log term failed | err=[%w]", err)
	}

	cmd, err := readCmd(reader)
	if err != nil {
		return nil, fmt.Errorf("Read log command failed | err=[%w]", err)
	}

	keyLen, err := readKVLen(reader)
	if err != nil {
		return nil, fmt.Errorf("Read log key length failed | err=[%w]", err)
	}

	key, err := readKey(reader, keyLen)
	if err != nil {
		return nil, fmt.Errorf("Read log key failed | err=[%w]", err)
	}

	log := &raftLog{
		entry: &pb.AppendEntriesReq_LogEntry{
			Index: index,
			Term:  term,
			Cmd:   cmd,
			Key:   []byte(key),
		},
	}

	if cmd == CmdPut {
		valLen, err := readKVLen(reader)
		if err != nil {
			return nil, fmt.Errorf("Read log value length failed | err=[%w]", err)
		}

		val, err := readValue(reader, valLen)
		if err != nil {
			return nil, fmt.Errorf("Read log value failed | err=[%w]", err)
		}

		log.entry.Val = []byte(val)
	}

	return log, nil
}

func readIndex(reader io.Reader) (uint64, error) {
	raw := make([]byte, unsafe.Sizeof(uint64(0)))
	if _, err := io.ReadFull(reader, raw); err != nil {
		return 0, fmt.Errorf("Read full failed | err=[%w]", err)
	}
	val := uint64(0)
	for i, b := range raw {
		val |= uint64(b) << (8 * i)
	}
	return val, nil
}

func readTerm(reader io.Reader) (uint64, error) {
	raw := make([]byte, unsafe.Sizeof(uint64(0)))
	if _, err := io.ReadFull(reader, raw); err != nil {
		return 0, fmt.Errorf("Read full failed | err=[%w]", err)
	}
	val := uint64(0)
	for i, b := range raw {
		val |= uint64(b) << (8 * i)
	}
	return val, nil
}

func readCmd(reader io.Reader) (uint32, error) {
	raw := make([]byte, unsafe.Sizeof(uint32(0)))
	if _, err := io.ReadFull(reader, raw); err != nil {
		return 0, fmt.Errorf("Read full failed | err=[%w]", err)
	}
	val := uint32(0)
	for i, b := range raw {
		val |= uint32(b) << (8 * i)
	}
	return val, nil
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

func (rl *raftLog) String() string {
	return fmt.Sprintf("[index=%v | term=%v | cmd=%v | key=%v | val=%v]",
		rl.entry.GetIndex(), rl.entry.GetTerm(), rl.entry.GetCmd(), rl.entry.GetKey(), rl.entry.GetVal())
}
