package ledger

import (
	"sync"
	"testing"

	"github.com/ileon/bft-consensus/crypto"
)

func testGenesisAndAddr(t *testing.T) (*Block, crypto.Address) {
	t.Helper()
	_, pub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	genesis := NewBlock(Header{Height: 0, Timestamp: 1, ParentHash: make([]byte, hashSize), Proposer: pub.Address()}, nil)
	return genesis, pub.Address()
}

func TestSequentialAppendsSucceedGapErrors(t *testing.T) {
	genesis, addr := testGenesisAndAddr(t)
	store := NewStore(genesis)

	b1 := NewBlock(Header{Height: 1, Timestamp: 2, ParentHash: genesis.Hash(), Proposer: addr}, nil)
	if err := store.Append(b1); err != nil {
		t.Fatalf("Append height 1: %v", err)
	}
	b2 := NewBlock(Header{Height: 2, Timestamp: 3, ParentHash: b1.Hash(), Proposer: addr}, nil)
	if err := store.Append(b2); err != nil {
		t.Fatalf("Append height 2: %v", err)
	}
	if store.Height() != 2 {
		t.Fatalf("expected height 2, got %d", store.Height())
	}

	b4 := NewBlock(Header{Height: 4, Timestamp: 5, ParentHash: b2.Hash(), Proposer: addr}, nil)
	if err := store.Append(b4); err != ErrHeightGap {
		t.Fatalf("expected ErrHeightGap for height gap, got %v", err)
	}
}

func TestAppendConflictingBlockAtExistingHeight(t *testing.T) {
	genesis, addr := testGenesisAndAddr(t)
	store := NewStore(genesis)

	b1 := NewBlock(Header{Height: 1, Timestamp: 2, ParentHash: genesis.Hash(), Proposer: addr}, nil)
	if err := store.Append(b1); err != nil {
		t.Fatalf("Append height 1: %v", err)
	}

	conflicting := NewBlock(Header{Height: 1, Timestamp: 99, ParentHash: genesis.Hash(), Proposer: addr}, nil)
	if err := store.Append(conflicting); err != ErrConflictingBlock {
		t.Fatalf("expected ErrConflictingBlock, got %v", err)
	}
}

func TestByHeightAndByHash(t *testing.T) {
	genesis, addr := testGenesisAndAddr(t)
	store := NewStore(genesis)
	b1 := NewBlock(Header{Height: 1, Timestamp: 2, ParentHash: genesis.Hash(), Proposer: addr}, nil)
	if err := store.Append(b1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, ok := store.ByHeight(1)
	if !ok || string(got.Hash()) != string(b1.Hash()) {
		t.Fatal("ByHeight(1) did not return the appended block")
	}
	got, ok = store.ByHash(b1.Hash())
	if !ok || string(got.Hash()) != string(b1.Hash()) {
		t.Fatal("ByHash did not return the appended block")
	}
	if _, ok := store.ByHeight(99); ok {
		t.Fatal("expected ByHeight of nonexistent height to fail")
	}
}

// TestConcurrentReadsDuringWriteRaceClean races Head/Height reads against
// a sequential series of Append writes. Only the test goroutine ever
// calls Append, so there is no ambiguity about which append should
// succeed — the -race detector is what actually exercises the RWMutex.
func TestConcurrentReadsDuringWriteRaceClean(t *testing.T) {
	genesis, addr := testGenesisAndAddr(t)
	store := NewStore(genesis)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = store.Head()
				_ = store.Height()
			}
		}
	}()

	prev := genesis
	for h := uint64(1); h <= 50; h++ {
		b := NewBlock(Header{Height: h, Timestamp: int64(h) + 1, ParentHash: prev.Hash(), Proposer: addr}, nil)
		if err := store.Append(b); err != nil {
			t.Fatalf("Append height %d: %v", h, err)
		}
		prev = b
	}
	close(stop)
	wg.Wait()
}
