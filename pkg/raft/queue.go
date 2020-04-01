package raft

import "fmt"

// persistTask represents a task to persist a raft log.
type persistTask struct {
	index   uint64    // Index of the log to persist.
	success chan bool // Channel to signal whether task succeeds.
	err     error     // Error value if task fails.
}

// newPersistTask creates a persistTask to persist raft log at given index.
func newPersistTask(index uint64) *persistTask {
	return &persistTask{
		index:   index,
		success: make(chan bool),
		err:     nil,
	}
}

// String returns a string representation of the task.
func (pt *persistTask) String() string {
	return fmt.Sprintf("[index=%v | err=[%v]]", pt.index, pt.err)
}

// persistQueue is a thread-safe blocking queue to store persistTask(s).
type persistQueue struct {
	queue chan *persistTask
}

// newPersistQueue creates a persistQueue with given size.
func newPersistQueue(size int) *persistQueue {
	return &persistQueue{
		queue: make(chan *persistTask, size),
	}
}

// push inserts a new task to the end of the queue.
func (pq *persistQueue) push(task *persistTask) {
	pq.queue <- task
}

// pop removes and returns the first task in the front of the queue.
func (pq *persistQueue) pop() chan *persistTask {
	return pq.queue
}

// close closes the queue. No further operations can be performed on the queue.
func (pq *persistQueue) close() {
	close(pq.queue)
}
