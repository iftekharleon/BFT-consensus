package ledger

import (
	"errors"
	"sync"
)

// ErrConflictingBlock is returned when Append is given a block at a height
// that is already occupied — a fork attempt (or a caller bug re-appending
// the same block). The store is the last line of defense against silently
// accepting a second block at the same height.
var ErrConflictingBlock = errors.New("ledger: conflicting block at existing height")

// ErrHeightGap is returned when Append is given a block whose height is
// not exactly Head().Height + 1. A gap means the caller (the consensus
// FSM) has a bug; failing loudly here catches it immediately instead of
// letting the chain silently diverge.
var ErrHeightGap = errors.New("ledger: block height is not head height + 1")

// Store is an append-only, in-memory store of committed blocks — the
// canonical chain as seen by this node. An in-memory backend is
// deliberately sufficient for this project's scope; the interface is
// narrow enough that a persistent backend (BadgerDB, Pebble, ...) could
// slot in later without changing any caller.
type Store struct {
	mu     sync.RWMutex
	blocks []*Block // blocks[h] is the block at height h
	byHash map[string]*Block
}

// NewStore creates a Store seeded with the genesis block at height 0.
func NewStore(genesis *Block) *Store {
	return &Store{
		blocks: []*Block{genesis},
		byHash: map[string]*Block{string(genesis.Hash()): genesis},
	}
}

// Append adds b to the store. b must be exactly one height above the
// current head; a block at an already-occupied height is rejected as
// ErrConflictingBlock, never silently accepted or ignored.
func (s *Store) Append(b *Block) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	headHeight := uint64(len(s.blocks) - 1)
	if b.Header.Height <= headHeight {
		return ErrConflictingBlock
	}
	if b.Header.Height != headHeight+1 {
		return ErrHeightGap
	}

	s.blocks = append(s.blocks, b)
	s.byHash[string(b.Hash())] = b
	return nil
}

// Head returns the most recently appended block.
func (s *Store) Head() *Block {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.blocks[len(s.blocks)-1]
}

// Height returns the height of the most recently appended block.
func (s *Store) Height() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return uint64(len(s.blocks) - 1)
}

// ByHeight looks up a block by height.
func (s *Store) ByHeight(h uint64) (*Block, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if h >= uint64(len(s.blocks)) {
		return nil, false
	}
	return s.blocks[h], true
}

// ByHash looks up a block by its hash.
func (s *Store) ByHash(hash []byte) (*Block, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.byHash[string(hash)]
	return b, ok
}
