package sim

import (
	"crypto/sha256"
	"sync"
	"time"

	"github.com/ileon/bft-consensus/consensus"
	"github.com/ileon/bft-consensus/crypto"
	"github.com/ileon/bft-consensus/ledger"
)

// Behavior controls what a SimNode actually sends, decoupled entirely
// from consensus.State: OnPropose/OnVote may duplicate, alter, or drop
// outgoing messages, and ShouldSend decides per-recipient whether each
// resulting message is delivered. Every fault below composes with every
// other — a partition combined with an equivocating proposer is a
// strictly harder scenario than either alone, and nothing here requires
// touching consensus/state.go to express it.
type Behavior interface {
	OnPropose(p *consensus.Proposal) []*consensus.Proposal
	OnVote(v *consensus.Vote) []*consensus.Vote
	ShouldSend(to crypto.Address, msg any) bool
}

// HonestBehavior passes every message through unchanged. It's the
// default for every node until MakeByzantine overrides it.
type HonestBehavior struct{}

func (HonestBehavior) OnPropose(p *consensus.Proposal) []*consensus.Proposal {
	return []*consensus.Proposal{p}
}
func (HonestBehavior) OnVote(v *consensus.Vote) []*consensus.Vote { return []*consensus.Vote{v} }
func (HonestBehavior) ShouldSend(crypto.Address, any) bool        { return true }

// SilentNode sends nothing at all. Must prove: the chain still commits
// with f silent nodes, via round-change liveness.
type SilentNode struct{}

func (SilentNode) OnPropose(*consensus.Proposal) []*consensus.Proposal { return nil }
func (SilentNode) OnVote(*consensus.Vote) []*consensus.Vote            { return nil }
func (SilentNode) ShouldSend(crypto.Address, any) bool                 { return false }

// EquivocatingProposer sends a different block to each half of the
// validator set for the same (height, round) — the headline attack.
// Must prove: safety holds regardless — no fork, AssertNoFork passes.
//
// Building a second, independently valid proposal requires signing it,
// so this behavior needs the proposer's own private key (supplied at
// construction, exactly as the sim harness already holds every node's
// keys when wiring up MakeByzantine).
type EquivocatingProposer struct {
	priv           crypto.PrivKey
	groupA, groupB map[crypto.Address]bool

	mu        sync.Mutex
	hashGroup map[string]bool // blockHash -> true means "groupA", false means "groupB"
}

func NewEquivocatingProposer(priv crypto.PrivKey, groupA, groupB []crypto.Address) *EquivocatingProposer {
	return &EquivocatingProposer{
		priv:      priv,
		groupA:    toSet(groupA),
		groupB:    toSet(groupB),
		hashGroup: make(map[string]bool),
	}
}

func (e *EquivocatingProposer) OnPropose(p *consensus.Proposal) []*consensus.Proposal {
	if p.Block == nil {
		return []*consensus.Proposal{p}
	}

	altHeader := p.Block.Header
	altHeader.Timestamp++ // trivial content change -> a genuinely different block hash
	altBlock := ledger.NewBlock(altHeader, p.Block.Txs)
	alt := &consensus.Proposal{
		Height: p.Height, Round: p.Round, BlockHash: altBlock.Hash(), Block: altBlock,
		POLRound: p.POLRound, Proposer: p.Proposer,
	}
	alt.Signature = e.priv.Sign(alt.SignBytes())

	e.mu.Lock()
	e.hashGroup[string(p.BlockHash)] = true
	e.hashGroup[string(alt.BlockHash)] = false
	e.mu.Unlock()

	return []*consensus.Proposal{p, alt}
}

func (e *EquivocatingProposer) OnVote(v *consensus.Vote) []*consensus.Vote {
	return []*consensus.Vote{v} // this node's own votes stay honest; only its proposal forks
}

func (e *EquivocatingProposer) ShouldSend(to crypto.Address, msg any) bool {
	prop, ok := msg.(*consensus.Proposal)
	if !ok {
		return true
	}
	e.mu.Lock()
	isGroupA, known := e.hashGroup[string(prop.BlockHash)]
	e.mu.Unlock()
	if !known {
		return true
	}
	if isGroupA {
		return e.groupA[to]
	}
	return e.groupB[to]
}

// DoubleVoter sends conflicting prevotes (or precommits) to different
// halves of the set — cryptographic proof of equivocation once any
// single node observes both. Must prove: equivocation detected, Evidence
// produced, no fork.
type DoubleVoter struct {
	priv           crypto.PrivKey
	groupA, groupB map[crypto.Address]bool

	mu        sync.Mutex
	hashGroup map[string]bool
}

func NewDoubleVoter(priv crypto.PrivKey, groupA, groupB []crypto.Address) *DoubleVoter {
	return &DoubleVoter{
		priv:      priv,
		groupA:    toSet(groupA),
		groupB:    toSet(groupB),
		hashGroup: make(map[string]bool),
	}
}

func (d *DoubleVoter) OnPropose(p *consensus.Proposal) []*consensus.Proposal {
	return []*consensus.Proposal{p}
}

func (d *DoubleVoter) OnVote(v *consensus.Vote) []*consensus.Vote {
	altHash := conflictingHash(v.BlockHash)
	alt := &consensus.Vote{
		Type: v.Type, Height: v.Height, Round: v.Round, BlockHash: altHash,
		Voter: v.Voter, Timestamp: v.Timestamp, ChainID: v.ChainID,
	}
	alt.Signature = d.priv.Sign(alt.SignBytes())

	d.mu.Lock()
	d.hashGroup[string(v.BlockHash)] = true
	d.hashGroup[string(altHash)] = false
	d.mu.Unlock()

	return []*consensus.Vote{v, alt}
}

func (d *DoubleVoter) ShouldSend(to crypto.Address, msg any) bool {
	vote, ok := msg.(*consensus.Vote)
	if !ok {
		return true
	}
	d.mu.Lock()
	isGroupA, known := d.hashGroup[string(vote.BlockHash)]
	d.mu.Unlock()
	if !known {
		return true
	}
	if isGroupA {
		return d.groupA[to]
	}
	return d.groupB[to]
}

// conflictingHash derives a plausible-looking but different 32-byte hash
// from orig. It doesn't need to correspond to a real block: VoteSet's
// equivocation check only ever compares the raw BlockHash bytes of two
// signed votes from the same validator, never resolves them to actual
// block content.
func conflictingHash(orig []byte) []byte {
	h := sha256.Sum256(append([]byte("sim-double-vote-fork:"), orig...))
	return h[:]
}

// LaggingNode delays every outbound message by Delay. Must prove: the
// chain tolerates slow peers, possibly needing extra rounds.
type LaggingNode struct {
	Delay time.Duration
}

func (l LaggingNode) OnPropose(p *consensus.Proposal) []*consensus.Proposal {
	time.Sleep(l.Delay)
	return []*consensus.Proposal{p}
}

func (l LaggingNode) OnVote(v *consensus.Vote) []*consensus.Vote {
	time.Sleep(l.Delay)
	return []*consensus.Vote{v}
}

func (l LaggingNode) ShouldSend(crypto.Address, any) bool { return true }

// InvalidProposer proposes a block with a corrupted parent hash. Must
// prove: honest nodes prevote NIL, round advances, chain recovers.
type InvalidProposer struct {
	priv crypto.PrivKey
}

func NewInvalidProposer(priv crypto.PrivKey) InvalidProposer { return InvalidProposer{priv: priv} }

func (i InvalidProposer) OnPropose(p *consensus.Proposal) []*consensus.Proposal {
	if p.Block == nil {
		return []*consensus.Proposal{p}
	}
	badHeader := p.Block.Header
	badHeader.ParentHash = conflictingHash(badHeader.ParentHash) // definitely wrong parent
	badBlock := ledger.NewBlock(badHeader, p.Block.Txs)
	bad := &consensus.Proposal{
		Height: p.Height, Round: p.Round, BlockHash: badBlock.Hash(), Block: badBlock,
		POLRound: p.POLRound, Proposer: p.Proposer,
	}
	bad.Signature = i.priv.Sign(bad.SignBytes())
	return []*consensus.Proposal{bad}
}

func (i InvalidProposer) OnVote(v *consensus.Vote) []*consensus.Vote { return []*consensus.Vote{v} }
func (i InvalidProposer) ShouldSend(crypto.Address, any) bool        { return true }

// CensoringNode strips all transactions from its own proposals. Must
// prove: transaction liveness degrades, but chain (block) liveness does
// not — a useful nuance, not a contradiction.
type CensoringNode struct {
	priv crypto.PrivKey
}

func NewCensoringNode(priv crypto.PrivKey) CensoringNode { return CensoringNode{priv: priv} }

func (c CensoringNode) OnPropose(p *consensus.Proposal) []*consensus.Proposal {
	if p.Block == nil || len(p.Block.Txs) == 0 {
		return []*consensus.Proposal{p}
	}
	emptyBlock := ledger.NewBlock(p.Block.Header, nil)
	censored := &consensus.Proposal{
		Height: p.Height, Round: p.Round, BlockHash: emptyBlock.Hash(), Block: emptyBlock,
		POLRound: p.POLRound, Proposer: p.Proposer,
	}
	censored.Signature = c.priv.Sign(censored.SignBytes())
	return []*consensus.Proposal{censored}
}

func (c CensoringNode) OnVote(v *consensus.Vote) []*consensus.Vote { return []*consensus.Vote{v} }
func (c CensoringNode) ShouldSend(crypto.Address, any) bool        { return true }

// ReplayNode re-sends an old, already-signed vote alongside every new
// one it casts. Must prove: stale messages are rejected by the (height,
// round) staleness check, with no state corruption.
type ReplayNode struct {
	mu   sync.Mutex
	seen []*consensus.Vote
}

func (r *ReplayNode) OnPropose(p *consensus.Proposal) []*consensus.Proposal {
	return []*consensus.Proposal{p}
}

func (r *ReplayNode) OnVote(v *consensus.Vote) []*consensus.Vote {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := []*consensus.Vote{v}
	if len(r.seen) > 0 {
		out = append(out, r.seen[0]) // replay the oldest remembered vote
	}
	r.seen = append(r.seen, v)
	const maxRemembered = 10
	if len(r.seen) > maxRemembered {
		r.seen = r.seen[1:]
	}
	return out
}

func (r *ReplayNode) ShouldSend(crypto.Address, any) bool { return true }
