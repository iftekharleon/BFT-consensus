package consensus

import (
	"testing"
	"time"
)

func TestScheduleDeliversExactTimeout(t *testing.T) {
	tm := NewTimeoutManager()
	defer tm.Stop()

	want := Timeout{Height: 5, Round: 2, Step: StepPrevote, Duration: 20 * time.Millisecond}
	tm.Schedule(want)

	select {
	case got := <-tm.Chan():
		if got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for timeout to fire")
	}
}

func TestScheduleCancelsPreviousTimer(t *testing.T) {
	tm := NewTimeoutManager()
	defer tm.Stop()

	stale := Timeout{Height: 1, Round: 0, Step: StepPropose, Duration: 20 * time.Millisecond}
	tm.Schedule(stale)

	fresh := Timeout{Height: 1, Round: 1, Step: StepPropose, Duration: 40 * time.Millisecond}
	tm.Schedule(fresh)

	select {
	case got := <-tm.Chan():
		if got != fresh {
			t.Fatalf("expected only the fresh timeout to fire, got %+v", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for timeout to fire")
	}

	select {
	case got := <-tm.Chan():
		t.Fatalf("expected no second fire, got %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestStopPreventsFurtherFires(t *testing.T) {
	tm := NewTimeoutManager()
	tm.Schedule(Timeout{Height: 1, Round: 0, Step: StepPropose, Duration: 20 * time.Millisecond})
	tm.Stop()

	select {
	case got := <-tm.Chan():
		t.Fatalf("expected no fire after Stop, got %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
}
