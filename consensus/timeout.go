package consensus

import (
	"sync"
	"time"
)

// Timeout is a scheduled per-step deadline. When it fires, the (Height,
// Round, Step) it carries lets the FSM verify the fire is still relevant
// before acting on it (see State.handleTimeoutLocked) — a timer scheduled
// for an old step that fires late must never be allowed to trigger a
// spurious round change.
type Timeout struct {
	Height   uint64
	Round    uint32
	Step     Step
	Duration time.Duration
}

// TimeoutManager schedules a single active per-step deadline and delivers
// its expiry as a Timeout value on Chan(). This is the entire liveness
// mechanism: without it, one silent leader halts the chain forever, since
// nothing else would ever move the protocol past a step nobody responded
// to.
type TimeoutManager struct {
	mu      sync.Mutex
	timer   *time.Timer
	outCh   chan Timeout
	stopped bool
}

// NewTimeoutManager creates a TimeoutManager with no timer scheduled.
func NewTimeoutManager() *TimeoutManager {
	return &TimeoutManager{outCh: make(chan Timeout, 1)}
}

// Schedule stops any previously scheduled timer and starts a new one for
// t. Only one timer is ever active at a time — a leaked previous timer is
// exactly what produces the stale-fire problem this type exists to
// avoid.
func (tm *TimeoutManager) Schedule(t Timeout) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.timer != nil {
		tm.timer.Stop()
	}
	if tm.stopped {
		return
	}

	tm.timer = time.AfterFunc(t.Duration, func() {
		tm.mu.Lock()
		stopped := tm.stopped
		tm.mu.Unlock()
		if stopped {
			return
		}
		select {
		case tm.outCh <- t:
		default:
			// outCh is capacity 1. Since Schedule always stops the
			// previous timer before arming a new one, at most one fire is
			// ever in flight; a full channel here means the consumer
			// hasn't drained the previous fire yet. Dropping rather than
			// blocking keeps a slow consumer from wedging this timer's
			// goroutine.
		}
	})
}

// Chan returns the channel Timeout values are delivered on.
func (tm *TimeoutManager) Chan() <-chan Timeout {
	return tm.outCh
}

// Stop cancels any active timer and prevents future Schedule calls from
// arming a new one.
func (tm *TimeoutManager) Stop() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.stopped = true
	if tm.timer != nil {
		tm.timer.Stop()
	}
}
