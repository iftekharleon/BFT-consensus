package consensus

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestBackoffDurationGrowsAndCaps(t *testing.T) {
	base := 100 * time.Millisecond

	if got := BackoffDuration(base, 0); got != base {
		t.Fatalf("round 0: expected base duration %v, got %v", base, got)
	}

	prev := BackoffDuration(base, 0)
	for r := uint32(1); r <= 6; r++ {
		got := BackoffDuration(base, r)
		if got <= prev {
			t.Fatalf("round %d: expected duration to grow, prev=%v got=%v", r, prev, got)
		}
		prev = got
	}

	capped := BackoffDuration(base, 100)
	wantCap := time.Duration(float64(base) * backoffCapMultiplier)
	if capped != wantCap {
		t.Fatalf("expected duration to be capped at %v, got %v", wantCap, capped)
	}
}

// wireClusterSilencing behaves like wireCluster except messages
// originating from any index in silent are never forwarded to peers —
// modeling a validator that sends nothing (SilentNode behavior, see
// PROJECT_SPEC.md §3.16), while still running its own FSM normally
// (including receiving and reacting to everyone else's messages).
func wireClusterSilencing(states []*State, silent map[int]bool) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := range states {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case msg := <-states[i].MsgOut():
					if silent[i] {
						continue
					}
					for j := range states {
						if j == i {
							continue
						}
						if msg.Proposal != nil {
							_ = states[j].HandleProposal(msg.Proposal)
						}
						if msg.Vote != nil {
							_ = states[j].HandleVote(msg.Vote)
						}
					}
				case <-done:
					return
				}
			}
		}(i)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case t := <-states[i].TimeoutChan():
					states[i].HandleTimeout(t)
				case <-done:
					return
				}
			}
		}(i)
	}
	return func() {
		close(done)
		wg.Wait()
	}
}

// TestSilentLeaderRoundChangeRecovery is M5's gate: a silent leader must
// not halt the chain. The round-0 proposer for height 1 sends nothing;
// the other three honest nodes must time out, round-change, and commit
// height 1 via a later round's (non-silent) proposer.
func TestSilentLeaderRoundChangeRecovery(t *testing.T) {
	const n = 4
	states := newNNodeCluster(t, n)

	proposerAddr := states[0].valSet.Proposer(1, 0).Address
	silentIdx := -1
	for i, s := range states {
		if s.self == proposerAddr {
			silentIdx = i
			break
		}
	}
	if silentIdx == -1 {
		t.Fatal("could not identify height 1 round 0 proposer among cluster states")
	}

	stopPump := wireClusterSilencing(states, map[int]bool{silentIdx: true})
	defer stopPump()

	var wgStart sync.WaitGroup
	for _, s := range states {
		wgStart.Add(1)
		go func(s *State) {
			defer wgStart.Done()
			s.Start()
		}(s)
	}
	defer func() {
		for _, s := range states {
			s.Stop()
		}
		wgStart.Wait()
	}()

	start := time.Now()

	// Every node except the silent one must still commit height 1 — the
	// silent leader must not halt honest progress.
	var wg sync.WaitGroup
	for i, s := range states {
		if i == silentIdx {
			continue
		}
		wg.Add(1)
		go func(i int, s *State) {
			defer wg.Done()
			select {
			case b := <-s.CommitOut():
				if b.Header.Height != 1 {
					t.Errorf("node %d: expected first commit at height 1, got %d", i, b.Header.Height)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("node %d: timed out waiting for height 1 to commit despite silent leader", i)
			}
		}(i, s)
	}
	wg.Wait()

	elapsed := time.Since(start)
	// Round 0 cannot conclude without the silent leader's proposal — the
	// other nodes only move on once their own propose timeout (base
	// testTimeout) fires. If height 1 committed faster than that, round 0
	// couldn't have actually been exercised as silent.
	if elapsed < testTimeout {
		t.Fatalf("commit happened in %v, faster than a single propose timeout (%v) — round 0 may not have been genuinely silenced", elapsed, testTimeout)
	}
}

// TestNoGoroutineLeakAfterStop verifies TimeoutManager.Stop leaves no
// goroutine behind. Uses a runtime.NumGoroutine() count rather than an
// external leak-detection dependency (goleak) — sufficient for this
// project's scope and keeps consensus dependency-free.
func TestNoGoroutineLeakAfterStop(t *testing.T) {
	runtime.GC()
	before := runtime.NumGoroutine()

	for i := 0; i < 50; i++ {
		tm := NewTimeoutManager()
		tm.Schedule(Timeout{Height: 1, Round: 0, Step: StepPropose, Duration: time.Hour})
		tm.Stop()
	}

	// Give any straggling timer-fire goroutines a moment to exit; Stop
	// guarantees the timer won't fire again, but a goroutine already
	// mid-flight when Stop was called needs a scheduling tick to unwind.
	deadline := time.Now().Add(time.Second)
	for {
		runtime.GC()
		after := runtime.NumGoroutine()
		if after <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine count grew from %d to %d after 50 schedule/stop cycles", before, after)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
