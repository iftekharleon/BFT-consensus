package consensus

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/ileon/bft-consensus/crypto"
	"github.com/ileon/bft-consensus/ledger"
	"github.com/ileon/bft-consensus/mempool"
	"github.com/ileon/bft-consensus/validator"
)

const testTimeout = 50 * time.Millisecond

func testGenesisBlock(t *testing.T, proposer crypto.Address) *ledger.Block {
	t.Helper()
	return ledger.NewBlock(ledger.Header{
		Height:     0,
		Timestamp:  1,
		ParentHash: make([]byte, 32),
		Proposer:   proposer,
	}, nil)
}

func newSingleNodeState(t *testing.T) *State {
	t.Helper()
	priv, pub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	valSet, err := validator.NewSet([]validator.Validator{
		{Address: pub.Address(), PubKey: pub, VotingPower: 1},
	})
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	return newTestState(t, valSet, priv, pub.Address())
}

// newTwoNodeState builds a State for a validator set of two, returning
// the State for "self" (the state under test). Self's own vote alone
// (power 1 of 2, threshold 2) never satisfies quorum, which is useful
// for isolating a single transition's effect without it cascading
// through a full propose/prevote/precommit/commit chain the way it
// would at N=1.
func newTwoNodeState(t *testing.T) *State {
	t.Helper()
	priv, pub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	_, otherPub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	valSet, err := validator.NewSet([]validator.Validator{
		{Address: pub.Address(), PubKey: pub, VotingPower: 1},
		{Address: otherPub.Address(), PubKey: otherPub, VotingPower: 1},
	})
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	return newTestState(t, valSet, priv, pub.Address())
}

func newTestState(t *testing.T, valSet *validator.Set, priv crypto.PrivKey, self crypto.Address) *State {
	t.Helper()
	genesis := testGenesisBlock(t, self)
	store := ledger.NewStore(genesis)
	pool := mempool.NewPool(1000)

	return NewState(Config{
		ValSet:  valSet,
		PrivKey: priv,
		Self:    self,
		Store:   store,
		Mempool: pool,
		ChainID: "test-chain",

		ProposeTimeout:   testTimeout,
		PrevoteTimeout:   testTimeout,
		PrecommitTimeout: testTimeout,
	})
}

// TestSingleNodeCommitsTenBlocksUnaided is the spec's explicit stop-and-fix
// gate for M3: at N=1 the chain must commit heights 1..10 unaided before
// any further FSM work is trusted.
func TestSingleNodeCommitsTenBlocksUnaided(t *testing.T) {
	s := newSingleNodeState(t)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Start()
	}()

	var committed []*ledger.Block
	for len(committed) < 10 {
		select {
		case b := <-s.CommitOut():
			committed = append(committed, b)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for commits")
		}
	}

	s.Stop()
	wg.Wait()

	// Stop is asynchronous relative to the drain loop (checked between
	// queued actions, not synchronously with the call to Stop), so the
	// store may have advanced a height or two past what we observed on
	// CommitOut by the time the goroutine actually exits. What matters
	// for this gate is that the first 10 commits form an unaided,
	// sequential chain starting at height 1.
	for i, b := range committed {
		if b.Header.Height != uint64(i+1) {
			t.Fatalf("commit %d: expected height %d, got %d", i, i+1, b.Header.Height)
		}
	}
	if s.store.Height() < 10 {
		t.Fatalf("expected store height >= 10, got %d", s.store.Height())
	}
}

// TestEnterProposeReProposesLockedBlock is the regression test for the
// most commonly skipped branch: a validator that has a lock MUST
// re-propose that exact block (with POLRound set to the lock's round)
// rather than building a fresh one.
func TestEnterProposeReProposesLockedBlock(t *testing.T) {
	s := newSingleNodeState(t)
	s.height = 5
	s.round = 3

	locked := ledger.NewBlock(ledger.Header{
		Height: 5, Timestamp: 100, ParentHash: s.store.Head().Hash(), Proposer: s.self,
	}, nil)
	s.lockedBlock = locked
	s.lockedRound = 2

	s.enterPropose(3)

	if s.proposal == nil {
		t.Fatal("expected a proposal to be produced")
	}
	if !bytes.Equal(s.proposal.BlockHash, locked.Hash()) {
		t.Fatal("expected re-proposed block to be the locked block, not a freshly built one")
	}
	if s.proposal.POLRound != 2 {
		t.Fatalf("expected POLRound to equal lockedRound (2), got %d", s.proposal.POLRound)
	}
}

// TestRoundChangePreservesLock is the regression test for the single most
// common way to break safety in this protocol family: clearing the lock
// on round change instead of only on commit.
//
// This uses a 2-validator set and deliberately picks a target round
// where self is NOT the proposer, so enterNewRound's tail call into
// enterPropose just schedules a timeout and returns rather than
// cascading through a full re-propose/prevote/precommit chain (which, at
// N=1, would immediately reach quorum again and legitimately advance
// lockedRound to the new round — correct behavior, but it would obscure
// exactly what this test wants to isolate: the moment-of-transition
// state, before any further progress).
func TestRoundChangePreservesLock(t *testing.T) {
	s := newTwoNodeState(t)
	s.height = 5
	s.step = StepPrecommit

	locked := ledger.NewBlock(ledger.Header{
		Height: 5, Timestamp: 100, ParentHash: s.store.Head().Hash(), Proposer: s.self,
	}, nil)
	s.lockedBlock = locked
	s.lockedRound = 1

	var targetRound uint32 = 1000 // sentinel to catch a failed search
	for r := uint32(2); r < 10; r++ {
		if s.valSet.Proposer(s.height, r).Address != s.self {
			targetRound = r
			break
		}
	}
	if targetRound == 1000 {
		t.Fatal("could not find a round where self is not the proposer")
	}

	s.enterNewRound(targetRound)

	if s.lockedBlock == nil || !bytes.Equal(s.lockedBlock.Hash(), locked.Hash()) {
		t.Fatal("expected lockedBlock to survive round change")
	}
	if s.lockedRound != 1 {
		t.Fatalf("expected lockedRound to remain 1, got %d", s.lockedRound)
	}
	if s.proposal != nil {
		t.Fatal("expected proposal to be cleared on round change")
	}
	if s.round != targetRound {
		t.Fatalf("expected round to advance to %d, got %d", targetRound, s.round)
	}
}

// TestLockedNodeRefusesConflictingProposalWithoutPOL verifies the
// lock-loyalty branch of enterPrevote: once locked on X, a validator must
// prevote NIL for a conflicting proposal Y unless Y carries a valid
// proof-of-lock-change from a round after the lock. It also confirms the
// lock itself is untouched by this — refusing to prevote for a conflict
// is not the same as unlocking (see enterPrecommit's NIL-majority case
// in state.go for why a NIL result must never unlock).
func TestLockedNodeRefusesConflictingProposalWithoutPOL(t *testing.T) {
	s := newSingleNodeState(t)
	s.height = 5
	s.round = 2
	s.step = StepPrevote

	locked := ledger.NewBlock(ledger.Header{
		Height: 5, Timestamp: 100, ParentHash: s.store.Head().Hash(), Proposer: s.self,
	}, nil)
	s.lockedBlock = locked
	s.lockedRound = 1

	conflicting := ledger.NewBlock(ledger.Header{
		Height: 5, Timestamp: 200, ParentHash: s.store.Head().Hash(), Proposer: s.self,
	}, nil)
	s.proposal = &Proposal{Height: 5, Round: 2, BlockHash: conflicting.Hash(), Block: conflicting, POLRound: -1}

	s.enterPrevote(2)

	vs := s.prevotes[2]
	ownVote := vs.votes[s.self]
	if ownVote == nil {
		t.Fatal("expected own prevote to be recorded")
	}
	if !ownVote.IsNil() {
		t.Fatal("expected locked node to prevote NIL for a conflicting proposal without a valid POL")
	}
	// Lock must still be intact even after the full N=1 cascade through
	// precommit (own NIL prevote instantly reaches quorum) — a NIL
	// precommit majority must never unlock.
	if s.lockedBlock == nil || !bytes.Equal(s.lockedBlock.Hash(), locked.Hash()) {
		t.Fatal("expected lock to remain intact")
	}
}

// TestLockedNodeUnlocksOnValidPOL verifies the unlock branch: a
// conflicting proposal carrying a 2f+1 prevote majority for itself from a
// round after the lock must be accepted, and the lock cleared.
//
// Uses a 2-validator set so self's own prevote alone (power 1 of 2,
// threshold 2) does not itself complete quorum in round 3 — isolating
// the moment enterPrevote unlocks, before anything downstream could
// legitimately re-lock.
func TestLockedNodeUnlocksOnValidPOL(t *testing.T) {
	priv, pub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	otherPriv, otherPub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	valSet, err := validator.NewSet([]validator.Validator{
		{Address: pub.Address(), PubKey: pub, VotingPower: 1},
		{Address: otherPub.Address(), PubKey: otherPub, VotingPower: 1},
	})
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	s := newTestState(t, valSet, priv, pub.Address())

	s.height = 5
	s.round = 3
	s.step = StepPrevote

	locked := ledger.NewBlock(ledger.Header{
		Height: 5, Timestamp: 100, ParentHash: s.store.Head().Hash(), Proposer: s.self,
	}, nil)
	s.lockedBlock = locked
	s.lockedRound = 1

	newBlock := ledger.NewBlock(ledger.Header{
		Height: 5, Timestamp: 200, ParentHash: s.store.Head().Hash(), Proposer: s.self,
	}, nil)

	// Simulate a genuine 2f+1 prevote majority for newBlock at round 2
	// (after the lock's round 1) — both validators prevote for it, which
	// is what a valid POL is actually built from.
	polRoundVS := s.ensureVoteSet(2, VoteTypePrevote)
	for _, signer := range []struct {
		priv crypto.PrivKey
		addr crypto.Address
	}{
		{priv, pub.Address()},
		{otherPriv, otherPub.Address()},
	} {
		v := &Vote{Type: VoteTypePrevote, Height: 5, Round: 2, BlockHash: newBlock.Hash(), Voter: signer.addr, ChainID: s.chainID}
		v.Signature = signer.priv.Sign(v.SignBytes())
		if _, err := polRoundVS.AddVote(v); err != nil {
			t.Fatalf("AddVote: %v", err)
		}
	}
	if _, ok := polRoundVS.HasTwoThirdsMajority(); !ok {
		t.Fatal("test setup error: expected a 2f+1 prevote majority at round 2")
	}

	s.proposal = &Proposal{Height: 5, Round: 3, BlockHash: newBlock.Hash(), Block: newBlock, POLRound: 2}

	s.enterPrevote(3)

	ownVote := s.prevotes[3].votes[s.self]
	if ownVote == nil || ownVote.IsNil() {
		t.Fatal("expected node to prevote for the new block once a valid POL unlocks it")
	}
	if !bytes.Equal(ownVote.BlockHash, newBlock.Hash()) {
		t.Fatal("expected prevote for the new (unlocked) block")
	}
	if s.lockedBlock != nil {
		t.Fatal("expected lock to be cleared once unlocked by a valid POL")
	}
}
