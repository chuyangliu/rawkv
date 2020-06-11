package raft

import (
	"errors"
	"fmt"
	"io"
	"unsafe"

	"github.com/chuyangliu/rawkv/pkg/store"
)

const (
	cmdPut uint16 = 0
	cmdDel uint16 = 1
)

type raftLog struct {
	cmd uint16
	key store.Key
	val store.Value
}

func readLog(reader io.Reader) (*raftLog, error) {

	cmd, err := readCmd(reader)
	if err != nil {
		if errors.Unwrap(err) == io.EOF {
			return nil, nil
		}
		return nil, fmt.Errorf("Read command failed | err=[%w]", err)
	}

	keyLen, err := readKVLen(reader)
	if err != nil {
		return nil, fmt.Errorf("Read key length failed | err=[%w]", err)
	}

	key, err := readKey(reader, keyLen)
	if err != nil {
		return nil, fmt.Errorf("Read key failed | err=[%w]", err)
	}

	log := &raftLog{
		cmd: cmd,
		key: key,
	}

	if cmd == cmdPut {
		valLen, err := readKVLen(reader)
		if err != nil {
			return nil, fmt.Errorf("Read value length failed | err=[%w]", err)
		}

		val, err := readValue(reader, valLen)
		if err != nil {
			return nil, fmt.Errorf("Read value failed | err=[%w]", err)
		}

		log.val = val
	}

	return log, nil
}

func readCmd(reader io.Reader) (uint16, error) {
	raw := make([]byte, unsafe.Sizeof(uint16(0)))
	if _, err := io.ReadFull(reader, raw); err != nil {
		return 0, fmt.Errorf("Read full failed | err=[%w]", err)
	}
	val := uint16(0)
	for i, b := range raw {
		val |= uint16(b) << (8 * i)
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
