package consensus

import (
	"bytes"

	"github.com/ileon/bft-consensus/crypto"
)

// VoteType distinguishes prevotes from precommits.
type VoteType uint8

// VoteType starts at 1, not 0, so a zero-value (uninitialized) Vote has an
// invalid Type and fails loudly rather than silently being treated as a
// prevote.
const (
	VoteTypePrevote VoteType = iota + 1
	VoteTypePrecommit
)

// Vote is a signed prevote or precommit for a (height, round). A nil or
// empty BlockHash is a NIL vote: a validator that saw no valid proposal
// (or whose round timed out) actively votes NIL rather than staying
// silent. NIL votes count toward the 2f+1 needed to conclude a round —
// silence stalls the protocol, an explicit NIL vote lets it progress.
type Vote struct {
	Type      VoteType
	Height    uint64
	Round     uint32
	BlockHash []byte // nil/empty == NIL vote
	Voter     crypto.Address
	Timestamp int64
	Signature []byte

	// ChainID is bound into SignBytes to prevent a validator's signature
	// on one chain from being replayed as a valid vote on a different
	// chain that happens to share the same validator key (e.g. a
	// testnet and mainnet keypair reuse). See PROJECT_SPEC.md §3.13
	// point 2.
	ChainID string
}

// SignBytes returns the canonical byte encoding of everything this vote
// cryptographically commits to, excluding Signature.
//
// Type is included specifically because omitting it is the textbook
// mistake in this layer: without it, a signature over a prevote for
// (H, R, blockHash) would also verify as a valid signature over a
// precommit for the identical (H, R, blockHash) — replaying a prevote as
// a precommit is an instant safety break, since it lets one signed
// message masquerade as two different kinds of commitment.
func (v *Vote) SignBytes() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(v.Type))
	writeUint64(buf, v.Height)
	writeUint32(buf, v.Round)
	writeLenPrefixed(buf, v.BlockHash)
	writeLenPrefixed(buf, []byte(v.Voter))
	writeInt64(buf, v.Timestamp)
	writeLenPrefixed(buf, []byte(v.ChainID))
	return buf.Bytes()
}

// Verify reports whether Signature is a valid signature over SignBytes()
// by pub.
func (v *Vote) Verify(pub crypto.PubKey) bool {
	return pub.Verify(v.SignBytes(), v.Signature)
}

// IsNil reports whether this is a NIL vote.
func (v *Vote) IsNil() bool {
	return len(v.BlockHash) == 0
}
