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

// newNNodeCluster builds n States sharing one validator set and one
// genesis block, each with its own independent store/mempool — modeling
// n separate physical nodes. It does not wire any transport between
// them; callers do that (see wireCluster), keeping this distinct from
// sim/harness.go's MemTransport, which lands in M7.
func newNNodeCluster(t *testing.T, n int) []*State {
	t.Helper()

	type keypair struct {
		priv crypto.PrivKey
		pub  crypto.PubKey
	}
	keys := make([]keypair, n)
	vals := make([]validator.Validator, n)
	for i := 0; i < n; i++ {
		priv, pub, err := crypto.GenerateKeyPair()
		if err != nil {
			t.Fatalf("GenerateKeyPair: %v", err)
		}
		keys[i] = keypair{priv, pub}
		vals[i] = validator.Validator{Address: pub.Address(), PubKey: pub, VotingPower: 1}
	}
	valSet, err := validator.NewSet(vals)
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}

	genesis := testGenesisBlock(t, vals[0].Address)

	states := make([]*State, n)
	for i := 0; i < n; i++ {
		store := ledger.NewStore(genesis)
		pool := mempool.NewPool(1000)
		states[i] = NewState(Config{
			ValSet:  valSet,
			PrivKey: keys[i].priv,
			Self:    keys[i].pub.Address(),
			Store:   store,
			Mempool: pool,
			ChainID: "test-chain",

			ProposeTimeout:   testTimeout,
			PrevoteTimeout:   testTimeout,
			PrecommitTimeout: testTimeout,
		})
	}
	return states
}

// wireCluster forwards every outbound message from each node's MsgOut to
// every other node's HandleProposal/HandleVote, modeling a fully-
// connected honest network in-process. It returns a stop function that
// halts the forwarding goroutines and waits for them to exit.
func wireCluster(states []*State) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := range states {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case msg := <-states[i].MsgOut():
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

		// Each node's TimeoutManager must be pumped independently of the
		// message forwarding above — a node that misses a message over
		// the network has no other way to recover. Forgetting this
		// wiring silently strands a node forever with no round-change
		// path (see TestFourNodeAllHonestCommitsAtRoundZero's history:
		// this exact gap made the test hang until this pump was added).
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

// TestFourNodeAllHonestCommitsAtRoundZero is M4's gate: a 4-node
// all-honest in-process cluster must commit several consecutive heights
// without needing any round change. There is no explicit "round" field
// recorded on a committed block (round is a per-attempt consensus
// artifact, not permanent chain data — see ledger.Header), so this is
// verified indirectly: with all nodes honest, wired, and started
// together, every height should commit well within a single propose
// timeout. A round change anywhere would blow well past the deadline
// used here.
func TestFourNodeAllHonestCommitsAtRoundZero(t *testing.T) {
	const n = 4
	const targetHeight = 5

	states := newNNodeCluster(t, n)
	stopPump := wireCluster(states)
	defer stopPump()

	var wgStart sync.WaitGroup
	for _, s := range states {
		wgStart.Add(1)
		go func(s *State) {
			defer wgStart.Done()
			s.Start()
		}(s)
	}

	commits := make([][]*ledger.Block, n)
	var wgCollect sync.WaitGroup
	for i, s := range states {
		wgCollect.Add(1)
		go func(i int, s *State) {
			defer wgCollect.Done()
			deadline := time.After(3 * time.Second)
			for len(commits[i]) < targetHeight {
				select {
				case b := <-s.CommitOut():
					commits[i] = append(commits[i], b)
				case <-deadline:
					return
				}
			}
		}(i, s)
	}
	wgCollect.Wait()

	for i, s := range states {
		s.mu.Lock()
		t.Logf("node %d self=%s: height=%d round=%d step=%d commits=%d pending=%d", i, s.self, s.height, s.round, s.step, len(commits[i]), len(s.pending))
		s.mu.Unlock()
	}

	for _, s := range states {
		s.Stop()
	}
	wgStart.Wait()

	for i, blocks := range commits {
		if len(blocks) < targetHeight {
			t.Fatalf("node %d: expected >= %d commits, got %d", i, targetHeight, len(blocks))
		}
	}

	// AssertNoFork, done by hand here (sim.Cluster.AssertNoFork formalizes
	// this in M7): every node must agree on the block hash at every
	// height.
	for h := 0; h < targetHeight; h++ {
		want := commits[0][h].Hash()
		for i := 1; i < n; i++ {
			if !bytes.Equal(commits[i][h].Hash(), want) {
				t.Fatalf("height %d: node 0 and node %d committed different blocks", h+1, i)
			}
		}
	}
}

// TestStaleTimeoutDiscarded verifies the (Height, Round, Step) staleness
// guard in handleTimeoutLocked: a timer that fires for a step the state
// machine has since moved past must be silently discarded, never treated
// as a live event. Without this guard, races between "we already moved
// on" and "the old timer already fired" produce spurious round changes.
func TestStaleTimeoutDiscarded(t *testing.T) {
	s := newSingleNodeState(t)
	s.height = 5
	s.round = 2
	s.step = StepPrevote

	s.HandleTimeout(Timeout{Height: 5, Round: 1, Step: StepPrevote, Duration: time.Millisecond})
	if s.round != 2 || s.step != StepPrevote {
		t.Fatal("expected stale-round timeout to be discarded")
	}

	s.HandleTimeout(Timeout{Height: 4, Round: 2, Step: StepPrevote, Duration: time.Millisecond})
	if s.height != 5 || s.step != StepPrevote {
		t.Fatal("expected stale-height timeout to be discarded")
	}

	s.HandleTimeout(Timeout{Height: 5, Round: 2, Step: StepPropose, Duration: time.Millisecond})
	if s.step != StepPrevote {
		t.Fatal("expected stale-step timeout to be discarded")
	}
}

// TestBufferedNextHeightProposalPromotedOnEntry is a direct regression
// test for the fix described on wireCluster/State.bufferedProposal: a
// proposal for height+1 (round 0) that arrives before this node has
// finished the current height must be buffered, then promoted into
// s.proposal the moment enterNewHeight reaches that height — not
// dropped, which would strand the node at enterCommit once it later
// learns the winning hash from votes alone.
//
// Uses a 2-validator set with the *other* validator as height 2's
// proposer (self is only ever a receiver here) — at N=1 self is always
// its own proposer, so entering the new height would immediately
// overwrite any buffered proposal with a freshly built one, which
// doesn't exercise the promotion path this test is isolating.
func TestBufferedNextHeightProposalPromotedOnEntry(t *testing.T) {
	// Address ordering (and therefore proposer rotation) depends on
	// generated key bytes, which are random. Regenerate deterministically
	// until "other" lands as height 2's proposer, rather than skipping
	// the test on an unlucky draw.
	var priv, otherPriv crypto.PrivKey
	var pub, otherPub crypto.PubKey
	var valSet *validator.Set
	for attempt := 0; attempt < 20; attempt++ {
		var err error
		priv, pub, err = crypto.GenerateKeyPair()
		if err != nil {
			t.Fatalf("GenerateKeyPair: %v", err)
		}
		otherPriv, otherPub, err = crypto.GenerateKeyPair()
		if err != nil {
			t.Fatalf("GenerateKeyPair: %v", err)
		}
		valSet, err = validator.NewSet([]validator.Validator{
			{Address: pub.Address(), PubKey: pub, VotingPower: 1},
			{Address: otherPub.Address(), PubKey: otherPub, VotingPower: 1},
		})
		if err != nil {
			t.Fatalf("NewSet: %v", err)
		}
		if valSet.Proposer(2, 0).Address == otherPub.Address() {
			break
		}
	}
	if valSet.Proposer(2, 0).Address != otherPub.Address() {
		t.Fatal("could not generate a keypair ordering where 'other' proposes height 2")
	}
	s := newTestState(t, valSet, priv, pub.Address())

	// Advance the store to height 1 first, so the buffered height-2
	// proposal below can correctly reference height 1's real hash as its
	// parent — exactly as a peer who's already one height ahead would
	// have built it.
	committed1 := ledger.NewBlock(ledger.Header{
		Height: 1, Timestamp: 500, ParentHash: s.store.Head().Hash(), Proposer: otherPub.Address(),
	}, nil)
	if err := s.store.Append(committed1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// This node itself is still "on height 1" (hasn't run enterCommit /
	// enterNewHeight yet) when the height-2 proposal arrives.
	s.height = 1
	s.round = 0
	s.step = StepPrecommit

	nextBlock := ledger.NewBlock(ledger.Header{
		Height: 2, Timestamp: 1000, ParentHash: committed1.Hash(), Proposer: otherPub.Address(),
	}, nil)
	prop := &Proposal{Height: 2, Round: 0, BlockHash: nextBlock.Hash(), Block: nextBlock, POLRound: -1, Proposer: otherPub.Address()}
	prop.Signature = otherPriv.Sign(prop.SignBytes())

	if err := s.handleProposalLocked(prop); err != nil {
		t.Fatalf("expected next-height proposal to be accepted for buffering, got %v", err)
	}
	if s.bufferedProposal == nil {
		t.Fatal("expected proposal to be buffered")
	}
	if s.proposal != nil {
		t.Fatal("expected s.proposal to remain unset until we actually reach that height")
	}

	s.enterNewHeight(2)

	if s.bufferedProposal != nil {
		t.Fatal("expected buffered proposal to be consumed")
	}
	if s.proposal == nil || !bytes.Equal(s.proposal.BlockHash, nextBlock.Hash()) {
		t.Fatal("expected buffered proposal to be promoted into s.proposal on entering its height")
	}
}
