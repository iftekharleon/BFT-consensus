package mempool

import (
	"sync"
	"testing"
)

func TestReapThenFailedRoundThenReapAgain(t *testing.T) {
	p := NewPool(10)
	if err := p.Add([]byte("tx1")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := p.Add([]byte("tx2")); err != nil {
		t.Fatalf("Add: %v", err)
	}

	first := p.Reap(10)
	if len(first) != 2 {
		t.Fatalf("expected 2 txs reaped, got %d", len(first))
	}

	// Simulate a failed round: nothing removed, reap again.
	second := p.Reap(10)
	if len(second) != 2 {
		t.Fatalf("expected reap after failed round to return the same 2 txs, got %d", len(second))
	}
	if string(first[0]) != string(second[0]) || string(first[1]) != string(second[1]) {
		t.Fatal("expected identical transactions across reaps")
	}
}

func TestDuplicateAddRejected(t *testing.T) {
	p := NewPool(10)
	if err := p.Add([]byte("tx1")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := p.Add([]byte("tx1")); err != ErrDuplicateTx {
		t.Fatalf("expected ErrDuplicateTx, got %v", err)
	}
}

func TestAddBeyondMaxTxsErrors(t *testing.T) {
	p := NewPool(2)
	if err := p.Add([]byte("tx1")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := p.Add([]byte("tx2")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := p.Add([]byte("tx3")); err != ErrPoolFull {
		t.Fatalf("expected ErrPoolFull, got %v", err)
	}
}

func TestRemoveDropsCommittedTxsAndAllowsReAdd(t *testing.T) {
	p := NewPool(10)
	tx1, tx2 := []byte("tx1"), []byte("tx2")
	if err := p.Add(tx1); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := p.Add(tx2); err != nil {
		t.Fatalf("Add: %v", err)
	}

	p.Remove([][]byte{tx1})
	if p.Size() != 1 {
		t.Fatalf("expected 1 tx remaining after Remove, got %d", p.Size())
	}
	remaining := p.Reap(10)
	if len(remaining) != 1 || string(remaining[0]) != "tx2" {
		t.Fatalf("expected tx2 to remain, got %v", remaining)
	}

	// tx1 was removed, so it should be re-addable (dedup set was cleared for it).
	if err := p.Add(tx1); err != nil {
		t.Fatalf("expected re-add of removed tx to succeed, got %v", err)
	}
}

func TestConcurrentAddIsRaceClean(t *testing.T) {
	p := NewPool(1000)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = p.Add([]byte{byte(i)})
		}(i)
	}
	wg.Wait()
	if p.Size() != 100 {
		t.Fatalf("expected 100 unique txs, got %d", p.Size())
	}
}
