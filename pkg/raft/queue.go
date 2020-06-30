package raft

import "fmt"

type persistTask struct {
	logIndex uint64
	done     chan bool
	err      error
}

func newPersistTask(logIndex uint64) *persistTask {
	return &persistTask{
		logIndex: logIndex,
		done:     make(chan bool),
		err:      nil,
	}
}

func (pt *persistTask) String() string {
	return fmt.Sprintf("[logIndex=%v | err=[%v]]", pt.logIndex, pt.err)
}

type persistQueue struct {
	queue chan *persistTask
}

func newPersistQueue(size int) *persistQueue {
	return &persistQueue{
		queue: make(chan *persistTask, size),
	}
}

func (pq *persistQueue) push(task *persistTask) {
	pq.queue <- task
}

func (pq *persistQueue) pop() chan *persistTask {
	return pq.queue
}
