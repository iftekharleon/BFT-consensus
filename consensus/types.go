// Package consensus implements the Tendermint/IBFT-style round state
// machine: types, votes, quorum aggregation, the FSM, and timeout
// management. See PROJECT_SPEC.md §3.3-3.7.
package consensus

import (
	"bytes"
	"encoding/binary"

	"github.com/ileon/bft-consensus/crypto"
	"github.com/ileon/bft-consensus/ledger"
)

// Step is a phase within a single consensus round.
type Step uint8

const (
	StepNewHeight Step = iota
	StepPropose
	StepPrevote
	StepPrecommit
	StepCommit
)

// Proposal is the block a validator proposes for a given (height, round),
// broadcast by that round's proposer.
type Proposal struct {
	Height    uint64
	Round     uint32
	BlockHash []byte
	Block     *ledger.Block
	POLRound  int32 // proof-of-lock round; -1 if none. int32, not uint32, so -1 is representable without a sentinel hack.
	Proposer  crypto.Address
	Signature []byte
}

// SignBytes returns the canonical byte encoding of everything about this
// proposal that is cryptographically committed to. It uses explicit
// big-endian field encoding in a fixed order — never encoding/json, whose
// map ordering and whitespace are not guaranteed stable across processes,
// which would make an otherwise-identical proposal produce different sign
// bytes on different nodes (a signature-malleability bug).
//
// Signature is excluded (obvious, but the single most common mistake in
// this layer). Height and Round are included specifically to prevent
// cross-height/cross-round replay: without them, a signature valid for
// this proposal would also verify as a signature over a different
// (height, round) pairing of the same block, which is a real replay
// vector, not a hypothetical one.
func (p *Proposal) SignBytes() []byte {
	buf := new(bytes.Buffer)
	writeUint64(buf, p.Height)
	writeUint32(buf, p.Round)
	writeLenPrefixed(buf, p.BlockHash)
	writeInt32(buf, p.POLRound)
	writeLenPrefixed(buf, []byte(p.Proposer))
	return buf.Bytes()
}

// Verify reports whether Signature is a valid signature over SignBytes()
// by pub.
func (p *Proposal) Verify(pub crypto.PubKey) bool {
	return pub.Verify(p.SignBytes(), p.Signature)
}

// writeUint64 appends v as 8 big-endian bytes.
func writeUint64(buf *bytes.Buffer, v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	buf.Write(b[:])
}

// writeUint32 appends v as 4 big-endian bytes.
func writeUint32(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

// writeInt32 appends v as 4 big-endian bytes, reinterpreting the signed
// bit pattern as unsigned so -1 (no POL) round-trips exactly.
func writeInt32(buf *bytes.Buffer, v int32) {
	writeUint32(buf, uint32(v))
}

// writeInt64 appends v as 8 big-endian bytes.
func writeInt64(buf *bytes.Buffer, v int64) {
	writeUint64(buf, uint64(v))
}

// writeLenPrefixed appends a 4-byte big-endian length prefix followed by
// field. Every variable-length field in a canonical encoding goes through
// this helper: without an explicit length prefix, concatenating two
// variable-length fields back to back is ambiguous — "AB"+"CD" and
// "A"+"BCD" produce identical bytes — which would let two different
// logical messages collide on the same sign bytes.
func writeLenPrefixed(buf *bytes.Buffer, field []byte) {
	writeUint32(buf, uint32(len(field)))
	buf.Write(field)
}
