package raft

import (
	"math/rand"
	"time"

	"github.com/chuyangliu/rawkv/pkg/logging"
)

// raftTimer maintains a timer with randomized timeout in [durationMin, durationMax].
type raftTimer struct {
	logger      *logging.Logger
	durationMin int64 // milliseconds
	durationMax int64 // milliseconds
	random      *rand.Rand
	timer       *time.Timer
}

func newRaftTimer(logLevel int, duration int64) *raftTimer {
	return &raftTimer{
		logger:      logging.New(logLevel),
		durationMin: duration,
		durationMax: duration,
		random:      nil,
		timer:       nil,
	}
}

func newRaftTimerRand(logLevel int, durationMin int64, durationMax int64) *raftTimer {
	return &raftTimer{
		logger:      logging.New(logLevel),
		durationMin: durationMin,
		durationMax: durationMax,
		random:      rand.New(rand.NewSource(time.Now().UnixNano())),
		timer:       nil,
	}
}

func (t *raftTimer) start() {
	t.timer = time.NewTimer(t.getDuration())
}

func (t *raftTimer) stop() {
	if t.timer != nil {
		t.timer.Stop()
	}
}

func (t *raftTimer) timeout() <-chan time.Time {
	return t.timer.C
}

func (t *raftTimer) getDuration() time.Duration {
	ms := t.durationMin
	if t.random != nil {
		ms = t.random.Int63n(t.durationMax-t.durationMin+1) + t.durationMin
	}
	return time.Duration(ms) * time.Millisecond
}
