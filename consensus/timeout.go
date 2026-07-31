package consensus

import (
	"math"
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

// backoffCapMultiplier bounds exponential timeout growth at 10x the base
// duration. Growth must be capped — otherwise a chain that's gone through
// several round changes takes exponentially longer to even ATTEMPT
// recovery, which turns a transient network blip into a near-permanent
// stall. 10x keeps late rounds patient without becoming glacial.
const backoffCapMultiplier = 10.0

// BackoffDuration returns the timeout duration for a given round: base
// for round 0, growing as base * 1.5^round and capped at
// backoffCapMultiplier * base. Growth is mandatory, not cosmetic — under
// a network partition or a genuinely slow validator set, a fixed
// per-step timeout causes endless round churn where no round is ever
// given enough time to actually converge before the next one begins.
func BackoffDuration(base time.Duration, round uint32) time.Duration {
	if round == 0 {
		return base
	}
	multiplier := math.Pow(1.5, float64(round))
	if multiplier > backoffCapMultiplier {
		multiplier = backoffCapMultiplier
	}
	return time.Duration(float64(base) * multiplier)
}
