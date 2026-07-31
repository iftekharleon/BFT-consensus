package consensus

import (
	"bytes"
	"errors"

	"github.com/ileon/bft-consensus/crypto"
	"github.com/ileon/bft-consensus/validator"
)

// ErrWrongVoteSet is returned when a vote's (height, round, type) does not
// match the VoteSet it was added to.
var ErrWrongVoteSet = errors.New("consensus: vote does not match this vote set's height/round/type")

// ErrNotAValidator is returned when a vote's Voter is not a member of the
// validator set.
var ErrNotAValidator = errors.New("consensus: voter is not a member of the validator set")

// ErrInvalidVoteSignature is returned when a vote's signature does not
// verify against its claimed voter's public key.
var ErrInvalidVoteSignature = errors.New("consensus: vote signature is invalid")

// ErrEquivocation indicates cryptographic proof of Byzantine behavior: two
// conflicting signed votes from the same validator for the same
// (height, round, type). Two signatures from one key over conflicting
// statements cannot be forged or explained away — it is undeniable.
var ErrEquivocation = errors.New("consensus: validator sent conflicting votes")

// Evidence pairs the two conflicting signed votes that prove equivocation.
type Evidence struct {
	VoteA, VoteB *Vote
}

// EquivocationError wraps ErrEquivocation with the Evidence that proves
// it, so callers can both check errors.Is(err, ErrEquivocation) and
// extract the underlying votes via errors.As.
type EquivocationError struct {
	Evidence Evidence
}

func (e *EquivocationError) Error() string { return ErrEquivocation.Error() }
func (e *EquivocationError) Unwrap() error { return ErrEquivocation }

// VoteSet collects votes for a single (height, round, type), tracks
// accumulated voting power per block hash, and detects both quorum
// formation and equivocation.
//
// VoteSet holds no lock of its own: it is only ever mutated from within
// consensus.State, which serializes all access behind its own mutex (see
// state.go). Adding a second lock here would be redundant and would
// invite a lock-ordering bug.
type VoteSet struct {
	height   uint64
	round    uint32
	voteType VoteType
	valSet   *validator.Set

	votes       map[crypto.Address]*Vote
	powerByHash map[string]int64

	maj23      []byte
	maj23Found bool
}

// NewVoteSet constructs an empty VoteSet for the given (height, round,
// type).
func NewVoteSet(h uint64, r uint32, t VoteType, vs *validator.Set) *VoteSet {
	return &VoteSet{
		height:      h,
		round:       r,
		voteType:    t,
		valSet:      vs,
		votes:       make(map[crypto.Address]*Vote),
		powerByHash: make(map[string]int64),
	}
}

// AddVote validates and adds v to the set. Validation runs cheapest and
// most-rejecting checks first: (height, round, type) match, voter
// membership, signature validity, then — only once a vote is confirmed
// authentic — the equivocation check.
//
// A duplicate, identical vote returns (false, nil): it is not an error,
// but it also does not add new power (the validator already voted). A
// second, different vote from an already-seen validator is equivocation
// and returns an *EquivocationError carrying both signed votes as proof.
func (vs *VoteSet) AddVote(v *Vote) (added bool, err error) {
	if v.Height != vs.height || v.Round != vs.round || v.Type != vs.voteType {
		return false, ErrWrongVoteSet
	}

	val, ok := vs.valSet.GetByAddress(v.Voter)
	if !ok {
		return false, ErrNotAValidator
	}

	if !v.Verify(val.PubKey) {
		return false, ErrInvalidVoteSignature
	}

	if existing, seen := vs.votes[v.Voter]; seen {
		if bytes.Equal(existing.BlockHash, v.BlockHash) {
			return false, nil
		}
		return false, &EquivocationError{Evidence: Evidence{VoteA: existing, VoteB: v}}
	}

	vs.votes[v.Voter] = v
	hashKey := string(v.BlockHash)
	vs.powerByHash[hashKey] += val.VotingPower

	if !vs.maj23Found && vs.valSet.HasQuorum(vs.powerByHash[hashKey]) {
		vs.maj23Found = true
		vs.maj23 = v.BlockHash
	}

	return true, nil
}

// HasTwoThirdsMajority returns the block hash that has accumulated 2f+1
// voting power, if any. The result is cached on first discovery — once a
// majority forms it can never un-form (validators don't retract votes),
// so recomputing it on every subsequent AddVote would be wasted O(n) work
// in a hot path.
func (vs *VoteSet) HasTwoThirdsMajority() ([]byte, bool) {
	return vs.maj23, vs.maj23Found
}

// HasTwoThirdsAny reports whether 2f+1 voting power has been cast in
// total, regardless of which hash (or NIL) it went to. This is distinct
// from HasTwoThirdsMajority: it means "the round has spoken, no single
// block won" and signals it's safe to move on, even though no block
// reached majority. Conflating the two is a liveness bug — a round that
// legitimately split its votes would otherwise never be recognized as
// concluded.
func (vs *VoteSet) HasTwoThirdsAny() bool {
	return vs.valSet.HasQuorum(vs.TotalPower())
}

// TotalPower returns the total voting power that has cast a vote in this
// set, summed across every hash (including NIL).
func (vs *VoteSet) TotalPower() int64 {
	var total int64
	for _, p := range vs.powerByHash {
		total += p
	}
	return total
}
