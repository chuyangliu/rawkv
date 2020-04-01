package raft

import (
	"math/rand"
	"time"
)

// raftTimer implements a timer that supports fixed or randomized durations.
type raftTimer struct {
	durationMin int64       // Minimum timer duration in milliseconds.
	durationMax int64       // Maximum timer duration in milliseconds.
	random      *rand.Rand  // Random number generator.
	timer       *time.Timer // Built-in timer.
}

// newRaftTimer creates a raftTimer with fixed duration.
func newRaftTimer(duration int64) *raftTimer {
	return &raftTimer{
		durationMin: duration,
		durationMax: duration,
		random:      nil,
		timer:       nil,
	}
}

// newRaftTimerRand creates a raftTimer with randomized duration.
func newRaftTimerRand(durationMin int64, durationMax int64) *raftTimer {
	return &raftTimer{
		durationMin: durationMin,
		durationMax: durationMax,
		random:      rand.New(rand.NewSource(time.Now().UnixNano())),
		timer:       nil,
	}
}

// start starts the timer.
func (t *raftTimer) start() {
	t.timer = time.NewTimer(t.nextDuration())
}

// stop stops the timer.
func (t *raftTimer) stop() {
	if t.timer != nil {
		t.timer.Stop()
	}
}

// timeout returns a channel that will be filled with current time once the timer's duration elapses.
func (t *raftTimer) timeout() <-chan time.Time {
	return t.timer.C
}

// nextDuration returns the next duration for the timer.
func (t *raftTimer) nextDuration() time.Duration {
	ms := t.durationMin
	if t.random != nil {
		ms = t.random.Int63n(t.durationMax-t.durationMin+1) + t.durationMin
	}
	return time.Duration(ms) * time.Millisecond
}
