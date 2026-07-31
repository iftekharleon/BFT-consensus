// Package validator holds the validator set, voting power, quorum threshold
// computation, and deterministic proposer selection. See PROJECT_SPEC.md §3.2.
package validator

import (
	"errors"
	"sort"

	"github.com/ileon/bft-consensus/crypto"
)

// Validator is a single member of the consensus validator set.
type Validator struct {
	Address     crypto.Address
	PubKey      crypto.PubKey
	VotingPower int64
}

// Set is the ordered validator set for a chain. Validators are sorted by
// address on construction so that every node independently derives the
// identical ordering from the same genesis input — divergent ordering would
// mean divergent proposer selection, which forks the chain instantly.
type Set struct {
	validators []Validator
	totalPower int64
	index      map[crypto.Address]*Validator
}

var (
	ErrEmptySet         = errors.New("validator: set must not be empty")
	ErrDuplicateAddress = errors.New("validator: duplicate address in set")
	ErrNonPositivePower = errors.New("validator: voting power must be positive")
)

// NewSet constructs a validator Set, sorting by address for determinism and
// rejecting inputs that would otherwise be a live vulnerability: an empty
// set, a duplicate address, or non-positive voting power (which could be
// used to smuggle a zero- or negative-weight validator into quorum math).
func NewSet(vals []Validator) (*Set, error) {
	if len(vals) == 0 {
		return nil, ErrEmptySet
	}

	sorted := make([]Validator, len(vals))
	copy(sorted, vals)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Address < sorted[j].Address
	})

	index := make(map[crypto.Address]*Validator, len(sorted))
	var total int64
	for i := range sorted {
		v := &sorted[i]
		if v.VotingPower <= 0 {
			return nil, ErrNonPositivePower
		}
		if _, exists := index[v.Address]; exists {
			return nil, ErrDuplicateAddress
		}
		index[v.Address] = v
		total += v.VotingPower
	}

	return &Set{
		validators: sorted,
		totalPower: total,
		index:      index,
	}, nil
}

// Size returns the number of validators in the set.
func (s *Set) Size() int {
	return len(s.validators)
}

// TotalPower returns the sum of voting power across all validators.
func (s *Set) TotalPower() int64 {
	return s.totalPower
}

// QuorumThreshold returns the minimum voting power required for a 2f+1
// quorum: ceil(2*total/3), computed as (2*total)/3 + 1 under integer
// division.
//
// Sanity check by hand: total=4 (N=4, f=1) -> (2*4)/3 + 1 = 8/3 + 1 = 2 + 1
// = 3 = 2f+1. Correct.
func (s *Set) QuorumThreshold() int64 {
	return (2*s.totalPower)/3 + 1
}

// HasQuorum reports whether the given accumulated power meets or exceeds
// the 2f+1 quorum threshold.
func (s *Set) HasQuorum(power int64) bool {
	return power >= s.QuorumThreshold()
}

// GetByAddress looks up a validator by address.
func (s *Set) GetByAddress(a crypto.Address) (*Validator, bool) {
	v, ok := s.index[a]
	return v, ok
}

// Proposer deterministically selects the proposer for (height, round) via
// simple round-robin rotation over the sorted validator set. This rotates
// the leader on every round change so a single faulty leader can never
// stall the chain forever.
//
// Production chains typically use weighted round-robin (higher voting
// power proposes more often, proportional to stake) rather than uniform
// rotation. That is deliberately out of scope here: it adds real
// complexity (accumulator-based selection, e.g. Tendermint's actual
// algorithm) without changing anything the simulation harness needs to
// prove about safety or liveness under Byzantine faults. See
// docs/design.md §7 for the full tradeoff discussion.
func (s *Set) Proposer(height uint64, round uint32) Validator {
	idx := (height + uint64(round)) % uint64(len(s.validators))
	return s.validators[idx]
}
