// Package ledger defines the Block structure, hash-chain linkage, block
// validation, and the append-only committed block store. See PROJECT_SPEC.md
// §3.8-3.9.
package ledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/ileon/bft-consensus/crypto"
)

// maxTxsPerBlock bounds block size. Unbounded block size is a trivial
// resource-exhaustion vector for any node that has to store or replay it.
const maxTxsPerBlock = 10_000

// hashSize is the length in bytes of a sha256 digest, used for both
// ParentHash and TxRoot.
const hashSize = sha256.Size

// Header is the portion of a Block that is hashed to produce the block
// hash. Transactions are bound in via TxRoot rather than hashed directly,
// so the hash cost stays constant regardless of tx count.
type Header struct {
	Height     uint64
	Timestamp  int64
	ParentHash []byte
	TxRoot     []byte
	Proposer   crypto.Address
}

// Block is the unit of commitment: a header plus the transactions it binds.
type Block struct {
	Header Header
	Txs    [][]byte
	hash   []byte // computed once at construction, then immutable
}

var (
	ErrInvalidHeight           = errors.New("ledger: height must be greater than zero")
	ErrInvalidParentHashLength = errors.New("ledger: parent hash must be 32 bytes")
	ErrInvalidProposerAddress  = errors.New("ledger: proposer address is malformed")
	ErrTooManyTxs              = errors.New("ledger: transaction count exceeds limit")

	ErrHeightMismatch         = errors.New("ledger: height is not parent height + 1")
	ErrParentHashMismatch     = errors.New("ledger: parent hash does not match parent block")
	ErrTimestampNotIncreasing = errors.New("ledger: timestamp must be greater than parent timestamp")
)

// NewBlock constructs a Block, computing TxRoot from txs (overwriting
// whatever was in h.TxRoot) and the block hash immediately. Computing the
// hash eagerly at construction — rather than lazily on first call to
// Hash() — means Block never mutates after construction, so concurrent
// reads of a *Block from multiple goroutines (a *Store hands out shared
// pointers) are race-free without needing a lock on the struct itself.
func NewBlock(h Header, txs [][]byte) *Block {
	h.TxRoot = ComputeTxRoot(txs)
	b := &Block{Header: h, Txs: txs}
	sum := sha256.Sum256(encodeHeader(b.Header))
	b.hash = sum[:]
	return b
}

// Hash returns the block's cached hash.
func (b *Block) Hash() []byte {
	return b.hash
}

// blockWire is Block's on-the-wire JSON shape: Header and Txs only.
// Hash is deliberately never serialized — see UnmarshalJSON.
type blockWire struct {
	Header Header
	Txs    [][]byte
}

// MarshalJSON encodes only Header and Txs. The cached hash field is
// unexported and would otherwise be silently dropped by encoding/json
// on the way out and left nil on the way in; without custom
// (Un)MarshalJSON, a Block that round-trips through JSON (as every
// Proposal does over the network) would come back with Hash() == nil
// and fail every downstream comparison against BlockHash.
func (b *Block) MarshalJSON() ([]byte, error) {
	return json.Marshal(blockWire{Header: b.Header, Txs: b.Txs})
}

// UnmarshalJSON reconstructs the block via NewBlock, which always
// recomputes TxRoot and the block hash from the actual Header/Txs rather
// than trusting any serialized value for them. This is not just a
// round-trip correctness fix: it also means a peer cannot lie about a
// block's hash or tx root in transit — both are independently
// re-derived from the data that was actually sent.
func (b *Block) UnmarshalJSON(data []byte) error {
	var w blockWire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*b = *NewBlock(w.Header, w.Txs)
	return nil
}

// ComputeTxRoot computes a simple binary Merkle root over the given
// transactions. A nil and an empty non-nil slice both produce the same
// root (32 zero bytes) — treating them differently would make logically
// identical blocks hash differently across nodes that happen to construct
// the slice one way or the other, which forks the chain over nothing.
func ComputeTxRoot(txs [][]byte) []byte {
	if len(txs) == 0 {
		return make([]byte, hashSize)
	}

	level := make([][]byte, len(txs))
	for i, tx := range txs {
		h := sha256.Sum256(tx)
		level[i] = h[:]
	}

	for len(level) > 1 {
		var next [][]byte
		for i := 0; i < len(level); i += 2 {
			left := level[i]
			right := left
			if i+1 < len(level) {
				right = level[i+1]
			}
			combined := make([]byte, 0, hashSize*2)
			combined = append(combined, left...)
			combined = append(combined, right...)
			h := sha256.Sum256(combined)
			next = append(next, h[:])
		}
		level = next
	}
	return level[0]
}

// encodeHeader canonically serializes a Header for hashing. Fields are
// written in a fixed order with explicit big-endian encoding and length
// prefixes on variable-length fields — never encoding/json, whose map
// ordering and whitespace are not guaranteed stable across processes and
// would make the block hash non-deterministic across nodes.
func encodeHeader(h Header) []byte {
	buf := new(bytes.Buffer)

	var heightBuf [8]byte
	binary.BigEndian.PutUint64(heightBuf[:], h.Height)
	buf.Write(heightBuf[:])

	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(h.Timestamp))
	buf.Write(tsBuf[:])

	writeLenPrefixed(buf, h.ParentHash)
	writeLenPrefixed(buf, h.TxRoot)
	writeLenPrefixed(buf, []byte(h.Proposer))

	return buf.Bytes()
}

func writeLenPrefixed(buf *bytes.Buffer, field []byte) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(field)))
	buf.Write(lenBuf[:])
	buf.Write(field)
}

// ValidateBasic performs self-contained checks that require no chain
// context: height is positive (genesis, height 0, is constructed directly
// and never passes through this path), parent hash is a well-formed
// sha256 length, proposer address decodes to a well-formed address, and
// tx count is within the configured limit.
func (b *Block) ValidateBasic() error {
	if b.Header.Height == 0 {
		return ErrInvalidHeight
	}
	if len(b.Header.ParentHash) != hashSize {
		return ErrInvalidParentHashLength
	}
	if !isWellFormedAddress(b.Header.Proposer) {
		return ErrInvalidProposerAddress
	}
	if len(b.Txs) > maxTxsPerBlock {
		return ErrTooManyTxs
	}
	return nil
}

func isWellFormedAddress(a crypto.Address) bool {
	decoded, err := hex.DecodeString(string(a))
	if err != nil {
		return false
	}
	return len(decoded) == 20
}

// ValidateAgainstParent performs contextual checks that require the
// parent block: height must be exactly parent height + 1, parent hash
// must match the parent's actual hash, and timestamp must strictly
// increase. Separating this from ValidateBasic keeps stateless and
// stateful validation independently testable.
func (b *Block) ValidateAgainstParent(parent *Block) error {
	if b.Header.Height != parent.Header.Height+1 {
		return ErrHeightMismatch
	}
	if !bytes.Equal(b.Header.ParentHash, parent.Hash()) {
		return ErrParentHashMismatch
	}
	if b.Header.Timestamp <= parent.Header.Timestamp {
		return ErrTimestampNotIncreasing
	}
	return nil
}
