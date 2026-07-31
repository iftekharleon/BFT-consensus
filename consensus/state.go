package consensus

import (
	"bytes"
	"encoding/hex"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ileon/bft-consensus/crypto"
	"github.com/ileon/bft-consensus/ledger"
	"github.com/ileon/bft-consensus/mempool"
	"github.com/ileon/bft-consensus/validator"
)

// mempoolReapMax bounds how many pending transactions a proposal pulls
// from the mempool per block.
const mempoolReapMax = 1000

// Message is an outbound consensus message queued for broadcast to
// peers. Exactly one of Proposal or Vote is set. This is distinct from
// p2p.Envelope (the wire-level type introduced in M6) — Message is the
// package-internal broadcast unit consensus.State hands to whatever
// transport (real network or in-memory sim) is driving it.
type Message struct {
	Proposal *Proposal
	Vote     *Vote
}

// Config bundles everything State needs to run a single validator's
// consensus instance. This is a consensus-local config, distinct from
// config.Config: the latter also carries node-level concerns (listen
// address, peer list) that the FSM itself has no business knowing about.
// cmd/node (M6) is what translates a loaded config.Config into this type
// when wiring a node together.
type Config struct {
	ValSet  *validator.Set
	PrivKey crypto.PrivKey
	Self    crypto.Address
	Store   *ledger.Store
	Mempool *mempool.Pool
	ChainID string

	ProposeTimeout   time.Duration
	PrevoteTimeout   time.Duration
	PrecommitTimeout time.Duration
}

// State is the consensus finite state machine. It consumes proposals,
// votes, and timeouts, and emits messages to broadcast and blocks to
// commit.
//
// A height (or round) boundary is never crossed by a direct function
// call — enterNewHeight and enterNewRound are only ever invoked via the
// pending-action queue (see enqueue/drainPending), never called
// directly from another enter* function. Without this, a validator whose
// own vote alone completes quorum (the single-validator case, but also
// any burst of self-sufficient progress) would recurse through
// enterCommit -> enterNewHeight -> enterPropose -> ... indefinitely
// within one call stack, holding s.mu the entire time: Start() would
// never return and Stop() would never be able to acquire the lock to
// interrupt it. Routing height/round transitions through the queue
// converts that unbounded recursion into unbounded but flat iteration in
// drainPending, which checks the lock-free `stopped` flag between every
// action — so Stop() (which never touches s.mu) can halt it promptly
// regardless of how deep in a drain the state machine currently is.
type State struct {
	height uint64
	round  uint32
	step   Step

	valSet  *validator.Set
	privKey crypto.PrivKey
	self    crypto.Address
	chainID string

	proposal    *Proposal
	lockedBlock *ledger.Block
	lockedRound int32
	validBlock  *ledger.Block
	validRound  int32

	prevotes   map[uint32]*VoteSet
	precommits map[uint32]*VoteSet

	store    *ledger.Store
	mempool  *mempool.Pool
	timeouts *TimeoutManager

	proposeTimeout   time.Duration
	prevoteTimeout   time.Duration
	precommitTimeout time.Duration

	msgOut    chan Message
	commitOut chan *ledger.Block

	pending []func()
	started bool
	stopped atomic.Bool
	mu      sync.Mutex
}

// NewState constructs a State ready to Start().
func NewState(cfg Config) *State {
	return &State{
		valSet:  cfg.ValSet,
		privKey: cfg.PrivKey,
		self:    cfg.Self,
		chainID: cfg.ChainID,

		store:   cfg.Store,
		mempool: cfg.Mempool,

		proposeTimeout:   cfg.ProposeTimeout,
		prevoteTimeout:   cfg.PrevoteTimeout,
		precommitTimeout: cfg.PrecommitTimeout,

		timeouts: NewTimeoutManager(),

		prevotes:   make(map[uint32]*VoteSet),
		precommits: make(map[uint32]*VoteSet),

		msgOut:    make(chan Message, 64),
		commitOut: make(chan *ledger.Block, 64),

		lockedRound: -1,
		validRound:  -1,
	}
}

// Start begins consensus at one height past whatever the store already
// holds. Deriving the starting height from the store (rather than always
// starting at 1) means a node that restarts with a non-empty store
// resumes rather than re-committing history it already has.
//
// Start does not return until Stop is called (or the drain is otherwise
// interrupted) for a self-sufficient validator set — e.g. N=1 — since
// such a node can commit indefinitely without ever needing external
// input. Callers that need to observe progress (tests, cmd/node) should
// run Start in its own goroutine and read CommitOut / MsgOut
// concurrently.
func (s *State) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return
	}
	s.started = true
	s.enqueue(func() { s.enterNewHeight(s.store.Height() + 1) })
	s.drainPending()
}

// Stop halts the timeout manager and signals any in-progress drain to
// stop at the next opportunity. Stop deliberately does not take s.mu: a
// drain in progress holds it for as long as it runs, and Stop must be
// callable — and take effect promptly — regardless of what the state
// machine happens to be doing at that moment.
func (s *State) Stop() {
	s.stopped.Store(true)
	s.timeouts.Stop()
}

// Height returns the current height.
func (s *State) Height() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.height
}

// Round returns the current round.
func (s *State) Round() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.round
}

// MsgOut is the channel of outbound messages to broadcast to peers.
func (s *State) MsgOut() <-chan Message {
	return s.msgOut
}

// CommitOut is the channel of blocks as they commit.
func (s *State) CommitOut() <-chan *ledger.Block {
	return s.commitOut
}

// HandleProposal processes a proposal received from the network (or, for
// this node's own proposals, is bypassed in favor of directly setting
// s.proposal — see enterPropose).
func (s *State) HandleProposal(p *Proposal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.handleProposalLocked(p)
	s.drainPending()
	return err
}

// HandleVote processes a vote received from the network.
func (s *State) HandleVote(v *Vote) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.handleVoteLocked(v)
	s.drainPending()
	return err
}

// HandleTimeout processes a fired Timeout.
func (s *State) HandleTimeout(t Timeout) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handleTimeoutLocked(t)
	s.drainPending()
}

// --- internal: all functions below assume s.mu is already held ---

// enqueue defers action to be run by drainPending rather than calling it
// directly. Used exclusively for enterNewHeight/enterNewRound — the two
// transitions that "cycle back" to the start of a bounded chain of
// forward-progress calls. See the State doc comment for why.
func (s *State) enqueue(action func()) {
	s.pending = append(s.pending, action)
}

// drainPending runs queued actions until none remain or Stop has been
// called. Each action may itself enqueue further actions (e.g. a commit
// enqueuing the next height); the loop picks those up on the next
// iteration, so height/round progression is unbounded iteration here,
// never unbounded recursion.
func (s *State) drainPending() {
	for len(s.pending) > 0 {
		if s.stopped.Load() {
			s.pending = nil
			return
		}
		next := s.pending[0]
		s.pending = s.pending[1:]
		next()
	}
}

func (s *State) handleProposalLocked(p *Proposal) error {
	// Rule: never move backwards. Reject any message for (height, round)
	// strictly less than current.
	if p.Height < s.height || (p.Height == s.height && p.Round < s.round) {
		return nil
	}
	if p.Height != s.height {
		// Future-height proposals are not buffered in this
		// implementation (only future-round votes/proposals within the
		// current height are); see docs/design.md §7.
		return nil
	}

	proposerVal, ok := s.valSet.GetByAddress(p.Proposer)
	if !ok {
		return ErrNotAValidator
	}
	expected := s.valSet.Proposer(p.Height, p.Round)
	if expected.Address != p.Proposer {
		return ErrWrongProposer
	}
	if !p.Verify(proposerVal.PubKey) {
		return ErrInvalidProposalSignature
	}
	if p.Block == nil || !bytes.Equal(p.Block.Hash(), p.BlockHash) {
		return ErrProposalBlockMismatch
	}

	if err := p.Block.ValidateBasic(); err != nil {
		s.maybeAdvancePastInvalidProposal(p.Round)
		return err
	}
	if err := p.Block.ValidateAgainstParent(s.store.Head()); err != nil {
		s.maybeAdvancePastInvalidProposal(p.Round)
		return err
	}

	if p.Round != s.round {
		// Future-round proposal: not buffered (see note above); it will
		// need to be resent once we reach that round.
		return nil
	}
	if s.proposal != nil {
		return nil // already have a proposal for this round
	}

	s.proposal = p
	s.logTransition("received-proposal", p.BlockHash)
	if s.step == StepPropose {
		s.enterPrevote(s.round)
	}
	return nil
}

// maybeAdvancePastInvalidProposal moves straight to prevote (which will
// prevote NIL, since s.proposal stays unset) rather than waiting out the
// full propose timeout once we've definitively determined this round's
// proposal is invalid.
func (s *State) maybeAdvancePastInvalidProposal(round uint32) {
	if round == s.round && s.step == StepPropose {
		s.enterPrevote(s.round)
	}
}

func (s *State) handleVoteLocked(v *Vote) error {
	if v.Height < s.height || (v.Height == s.height && v.Round < s.round) {
		return nil
	}
	if v.Height != s.height {
		return nil
	}

	vs := s.ensureVoteSet(v.Round, v.Type)
	added, err := vs.AddVote(v)
	if err != nil {
		return err
	}
	if !added || v.Round != s.round {
		return nil
	}

	switch v.Type {
	case VoteTypePrevote:
		if s.step == StepPrevote {
			if _, ok := vs.HasTwoThirdsMajority(); ok {
				s.enterPrecommit(s.round)
			}
		}
	case VoteTypePrecommit:
		if s.step == StepPrecommit {
			if hash, ok := vs.HasTwoThirdsMajority(); ok {
				if len(hash) > 0 {
					s.enterCommit(s.round, hash)
				} else {
					round := s.round
					s.enqueue(func() { s.enterNewRound(round + 1) })
				}
			}
		}
	}
	return nil
}

func (s *State) handleTimeoutLocked(t Timeout) {
	// Guard against stale fires: a timer scheduled for a step we've
	// since moved past must never trigger a spurious transition.
	if t.Height != s.height || t.Round != s.round || t.Step != s.step {
		return
	}
	switch t.Step {
	case StepPropose:
		s.enterPrevote(s.round)
	case StepPrevote:
		s.enterPrecommit(s.round)
	case StepPrecommit:
		hash, ok := s.ensureVoteSet(s.round, VoteTypePrecommit).HasTwoThirdsMajority()
		if ok && len(hash) > 0 {
			s.enterCommit(s.round, hash)
		} else {
			round := s.round
			s.enqueue(func() { s.enterNewRound(round + 1) })
		}
	}
}

func (s *State) enterNewHeight(h uint64) {
	s.height = h
	s.round = 0
	s.step = StepNewHeight
	s.lockedBlock = nil
	s.lockedRound = -1
	s.validBlock = nil
	s.validRound = -1
	s.proposal = nil
	s.prevotes = make(map[uint32]*VoteSet)
	s.precommits = make(map[uint32]*VoteSet)
	s.logTransition("new-height", nil)
	s.enterPropose(0)
}

func (s *State) enterPropose(round uint32) {
	s.round = round
	s.step = StepPropose
	s.ensureVoteSet(round, VoteTypePrevote)
	s.ensureVoteSet(round, VoteTypePrecommit)
	s.logTransition("enter-propose", nil)

	proposer := s.valSet.Proposer(s.height, round)
	if proposer.Address != s.self {
		s.timeouts.Schedule(Timeout{Height: s.height, Round: round, Step: StepPropose, Duration: s.proposeTimeout})
		return
	}

	var block *ledger.Block
	polRound := int32(-1)
	if s.lockedBlock != nil {
		// MUST re-propose the lock — skipping this breaks safety under
		// round change.
		block = s.lockedBlock
		polRound = s.lockedRound
	} else {
		parent := s.store.Head()
		txs := s.mempool.Reap(mempoolReapMax)
		ts := time.Now().UnixNano()
		if ts <= parent.Header.Timestamp {
			ts = parent.Header.Timestamp + 1
		}
		header := ledger.Header{
			Height:     s.height,
			Timestamp:  ts,
			ParentHash: parent.Hash(),
			Proposer:   s.self,
		}
		block = ledger.NewBlock(header, txs)
	}

	prop := &Proposal{
		Height:    s.height,
		Round:     round,
		BlockHash: block.Hash(),
		Block:     block,
		POLRound:  polRound,
		Proposer:  s.self,
	}
	prop.Signature = s.privKey.Sign(prop.SignBytes())

	s.proposal = prop
	s.broadcast(Message{Proposal: prop})
	s.timeouts.Schedule(Timeout{Height: s.height, Round: round, Step: StepPropose, Duration: s.proposeTimeout})
	s.enterPrevote(round)
}

func (s *State) enterPrevote(round uint32) {
	if round != s.round {
		return
	}
	s.step = StepPrevote

	var voteHash []byte
	switch {
	case s.proposal == nil:
		voteHash = nil
	case s.lockedBlock != nil && !bytes.Equal(s.lockedBlock.Hash(), s.proposal.BlockHash):
		if s.proposalHasValidPOL(s.proposal) {
			s.lockedBlock = nil
			s.lockedRound = -1
			voteHash = s.proposal.BlockHash
		} else {
			voteHash = nil // stay loyal to the lock
		}
	default:
		voteHash = s.proposal.BlockHash
	}

	s.logTransition("enter-prevote", voteHash)
	s.timeouts.Schedule(Timeout{Height: s.height, Round: round, Step: StepPrevote, Duration: s.prevoteTimeout})
	s.signAndSendVote(VoteTypePrevote, voteHash)
}

func (s *State) enterPrecommit(round uint32) {
	if round != s.round {
		return
	}
	s.step = StepPrecommit

	vs := s.ensureVoteSet(round, VoteTypePrevote)
	hash, ok := vs.HasTwoThirdsMajority()

	var precommitHash []byte
	switch {
	case !ok:
		precommitHash = nil
	case len(hash) == 0:
		// A NIL prevote majority means this round produced no result —
		// it is not proof that 2f+1 nodes abandoned the locked block.
		// Deliberately NOT unlocking here, even though a naive reading
		// of "hash is NIL -> unlock" pseudocode suggests it: unlocking
		// on NIL would let this node later precommit some different
		// block Y without ever having seen a genuine POL (a 2f+1
		// prevote majority for Y specifically) proving the network
		// moved on from the lock. That breaks exactly the
		// quorum-intersection argument the lock exists to guarantee
		// (see docs/design.md §4). Per docs/design.md §3 / the original
		// spec's own safety-mechanism section: "Unlocking happens only
		// on seeing a 2f+1 prevote set (a polka) for a *different
		// block*" — NIL is not a block.
		precommitHash = nil
	default:
		block := s.blockForHash(hash)
		s.lockedBlock = block
		s.lockedRound = int32(round)
		s.validBlock = block
		s.validRound = int32(round)
		precommitHash = hash
	}

	s.logTransition("enter-precommit", precommitHash)
	s.timeouts.Schedule(Timeout{Height: s.height, Round: round, Step: StepPrecommit, Duration: s.precommitTimeout})
	s.signAndSendVote(VoteTypePrecommit, precommitHash)
}

func (s *State) enterCommit(round uint32, hash []byte) {
	if round != s.round {
		return
	}
	s.step = StepCommit

	block := s.blockForHash(hash)
	if block == nil {
		return
	}
	if err := s.store.Append(block); err != nil {
		return
	}
	s.mempool.Remove(block.Txs)
	s.logTransition("commit", hash)

	select {
	case s.commitOut <- block:
	default:
	}

	nextHeight := s.height + 1
	s.enqueue(func() { s.enterNewHeight(nextHeight) })
}

func (s *State) enterNewRound(round uint32) {
	s.round = round
	s.step = StepPropose
	s.proposal = nil
	// lockedBlock/lockedRound deliberately NOT cleared here — the lock
	// survives round change and clears only on commit (enterNewHeight).
	s.logTransition("round-change", nil)
	s.enterPropose(round)
}

// signAndSendVote constructs, signs, broadcasts, and self-delivers a
// vote. Self-delivery matters: without it, this node never counts its
// own vote toward quorum unless the transport loops broadcasts back to
// the sender, which real networks don't do — the classic way to end up
// stuck at round 0 forever.
func (s *State) signAndSendVote(t VoteType, hash []byte) {
	v := &Vote{
		Type:      t,
		Height:    s.height,
		Round:     s.round,
		BlockHash: hash,
		Voter:     s.self,
		Timestamp: time.Now().Unix(),
		ChainID:   s.chainID,
	}
	v.Signature = s.privKey.Sign(v.SignBytes())
	s.broadcast(Message{Vote: v})
	_ = s.handleVoteLocked(v)
}

func (s *State) broadcast(m Message) {
	select {
	case s.msgOut <- m:
	default:
		// Non-blocking: a slow or absent consumer must never stall the
		// consensus loop.
	}
}

func (s *State) ensureVoteSet(round uint32, t VoteType) *VoteSet {
	m := s.prevotes
	if t == VoteTypePrecommit {
		m = s.precommits
	}
	vs, ok := m[round]
	if !ok {
		vs = NewVoteSet(s.height, round, t, s.valSet)
		m[round] = vs
	}
	return vs
}

func (s *State) blockForHash(hash []byte) *ledger.Block {
	if s.proposal != nil && bytes.Equal(s.proposal.BlockHash, hash) {
		return s.proposal.Block
	}
	return nil
}

func (s *State) proposalHasValidPOL(p *Proposal) bool {
	if p.POLRound < 0 || p.POLRound <= s.lockedRound {
		return false
	}
	vs, ok := s.prevotes[uint32(p.POLRound)]
	if !ok {
		return false
	}
	hash, ok := vs.HasTwoThirdsMajority()
	if !ok || len(hash) == 0 {
		return false
	}
	return bytes.Equal(hash, p.BlockHash)
}

func (s *State) logTransition(action string, blockHash []byte) {
	slog.Info("consensus transition",
		"height", s.height,
		"round", s.round,
		"step", s.step,
		"action", action,
		"blockHash", shortHash(blockHash),
	)
}

func shortHash(h []byte) string {
	if len(h) == 0 {
		return "nil"
	}
	n := 8
	if len(h) < n {
		n = len(h)
	}
	return hex.EncodeToString(h[:n])
}
