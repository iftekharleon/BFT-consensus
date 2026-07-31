// Package mempool buffers pending transactions and supplies them to block
// proposals. Deliberately minimal — no gas pricing, no nonce ordering, no
// eviction policy. A production mempool adds all of that; this project's
// interesting part is consensus, not fee markets. See PROJECT_SPEC.md
// §3.10.
package mempool

import (
	"crypto/sha256"
	"errors"
	"sync"
)

var (
	ErrDuplicateTx = errors.New("mempool: transaction already in pool")
	ErrPoolFull    = errors.New("mempool: pool is full")
)

// Pool is a bounded, deduplicated queue of pending transactions.
type Pool struct {
	mu     sync.Mutex
	txs    [][]byte
	seen   map[[32]byte]struct{}
	maxTxs int
}

// NewPool creates an empty pool that rejects additions once it holds
// maxTxs transactions — an unbounded mempool is a trivial memory-
// exhaustion DoS vector.
func NewPool(maxTxs int) *Pool {
	return &Pool{
		txs:    make([][]byte, 0),
		seen:   make(map[[32]byte]struct{}),
		maxTxs: maxTxs,
	}
}

// Add appends tx to the pool, rejecting duplicates (by sha256 of the raw
// bytes) and rejecting once the pool is at capacity.
func (p *Pool) Add(tx []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	h := sha256.Sum256(tx)
	if _, dup := p.seen[h]; dup {
		return ErrDuplicateTx
	}
	if len(p.txs) >= p.maxTxs {
		return ErrPoolFull
	}

	p.seen[h] = struct{}{}
	p.txs = append(p.txs, tx)
	return nil
}

// Reap returns up to max pending transactions without removing them from
// the pool. Peeking rather than removing is deliberate: if the round that
// reaped these transactions fails to commit (a round change, a silent
// leader, a failed proposal), the transactions must still be available
// for the next round's proposal. Removing on reap would silently drop
// transactions on every failed round — a subtle, very real bug class the
// spec calls out explicitly.
func (p *Pool) Reap(max int) [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()

	n := max
	if n > len(p.txs) {
		n = len(p.txs)
	}
	out := make([][]byte, n)
	copy(out, p.txs[:n])
	return out
}

// Remove drops the given transactions from the pool. Called once a block
// containing them actually commits.
func (p *Pool) Remove(txs [][]byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	toRemove := make(map[[32]byte]struct{}, len(txs))
	for _, tx := range txs {
		toRemove[sha256.Sum256(tx)] = struct{}{}
	}

	remaining := p.txs[:0]
	for _, tx := range p.txs {
		h := sha256.Sum256(tx)
		if _, drop := toRemove[h]; drop {
			delete(p.seen, h)
			continue
		}
		remaining = append(remaining, tx)
	}
	p.txs = remaining
}

// Size returns the number of transactions currently pending.
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.txs)
}
