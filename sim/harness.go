// Package sim provides an in-process N-node cluster harness with an
// in-memory transport and Byzantine fault injectors, used to prove safety
// and liveness under attack. See PROJECT_SPEC.md §3.15-3.17.
package sim

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ileon/bft-consensus/consensus"
	"github.com/ileon/bft-consensus/crypto"
	"github.com/ileon/bft-consensus/ledger"
	"github.com/ileon/bft-consensus/mempool"
	"github.com/ileon/bft-consensus/validator"
)

// simTimeout is the per-step timeout used for every simulated node.
// Short, since MemTransport delivers in-process with no real network
// delay — scenarios should resolve in milliseconds, not seconds.
const simTimeout = 30 * time.Millisecond

// SimNode is one validator's full local stack: its consensus.State plus
// the store/mempool/keys backing it, and the Behavior currently
// controlling what it sends (HonestBehavior by default).
type SimNode struct {
	Address crypto.Address
	PrivKey crypto.PrivKey
	State   *consensus.State
	Store   *ledger.Store
	Mempool *mempool.Pool

	inbox chan consensus.Message

	mu       sync.Mutex
	behavior Behavior
}

func (n *SimNode) setBehavior(b Behavior) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.behavior = b
}

func (n *SimNode) getBehavior() Behavior {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.behavior == nil {
		return HonestBehavior{}
	}
	return n.behavior
}

// Cluster is an in-process N-node BFT cluster. It exists specifically to
// make fault injection and partition testing trivial and fast: no real
// sockets, no sleeps to wait for network I/O, and every node's Behavior
// hook can be swapped per-test.
type Cluster struct {
	t         *testing.T
	nodes     []*SimNode
	transport *memTransport
	valSet    *validator.Set

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewCluster builds n validators sharing one validator set and one
// genesis block, wired to an in-memory transport. It does not start
// them — call Start.
func NewCluster(t *testing.T, n int) *Cluster {
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

	genesis := ledger.NewBlock(ledger.Header{
		Height: 0, Timestamp: 1, ParentHash: make([]byte, 32), Proposer: vals[0].Address,
	}, nil)

	transport := newMemTransport()
	nodes := make([]*SimNode, n)
	for i := 0; i < n; i++ {
		store := ledger.NewStore(genesis)
		pool := mempool.NewPool(1000)
		st := consensus.NewState(consensus.Config{
			ValSet:  valSet,
			PrivKey: keys[i].priv,
			Self:    keys[i].pub.Address(),
			Store:   store,
			Mempool: pool,
			ChainID: "sim-chain",

			ProposeTimeout:   simTimeout,
			PrevoteTimeout:   simTimeout,
			PrecommitTimeout: simTimeout,
		})
		node := &SimNode{
			Address: keys[i].pub.Address(),
			PrivKey: keys[i].priv,
			State:   st,
			Store:   store,
			Mempool: pool,
		}
		node.inbox = transport.register(node.Address)
		nodes[i] = node
	}

	return &Cluster{t: t, nodes: nodes, transport: transport, valSet: valSet, stopCh: make(chan struct{})}
}

// Node returns the i-th node.
func (c *Cluster) Node(i int) *SimNode { return c.nodes[i] }

// MakeByzantine replaces node i's outbound Behavior. All fault behavior
// lives here, in the transport/behavior layer — consensus.State itself
// never branches on whether a node is "supposed to" misbehave, so every
// scenario proves something about the real, unmodified consensus code
// path.
func (c *Cluster) MakeByzantine(i int, behavior Behavior) {
	c.nodes[i].setBehavior(behavior)
}

// Partition splits the cluster into two groups by node index; messages
// between different groups are dropped until HealPartition. Groups take
// effect immediately — in-flight pump goroutines consult the transport's
// partition state on every delivery attempt.
func (c *Cluster) Partition(groupA, groupB []int) {
	c.transport.partition(c.addressesFor(groupA), c.addressesFor(groupB))
}

// HealPartition removes any active partition.
func (c *Cluster) HealPartition() {
	c.transport.heal()
}

func (c *Cluster) addressesFor(idxs []int) []crypto.Address {
	addrs := make([]crypto.Address, len(idxs))
	for i, idx := range idxs {
		addrs[i] = c.nodes[idx].Address
	}
	return addrs
}

// SubmitTx adds tx to every node's mempool, simulating a client that
// broadcast the transaction to the whole network — this project doesn't
// implement mempool gossip between nodes (out of scope), so the harness
// stands in for that here.
func (c *Cluster) SubmitTx(tx []byte) {
	for _, n := range c.nodes {
		_ = n.Mempool.Add(tx)
	}
}

// Start begins every node's consensus loop and the pump goroutines that
// connect it to the in-memory transport.
func (c *Cluster) Start() {
	for _, n := range c.nodes {
		n := n
		c.wg.Add(4)
		go func() { defer c.wg.Done(); n.State.Start() }()
		go func() { defer c.wg.Done(); c.pumpOut(n) }()
		go func() { defer c.wg.Done(); c.pumpIn(n) }()
		go func() { defer c.wg.Done(); c.pumpTimeout(n) }()
	}
}

// Stop halts every node and waits for all pump goroutines to exit.
func (c *Cluster) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
	for _, n := range c.nodes {
		n.State.Stop()
	}
	c.wg.Wait()
}

func (c *Cluster) pumpOut(n *SimNode) {
	for {
		select {
		case msg := <-n.State.MsgOut():
			c.dispatch(n, msg)
		case <-c.stopCh:
			return
		}
	}
}

// dispatch is the fault-injection point: every outbound message passes
// through the sender's Behavior before delivery. OnPropose/OnVote may
// duplicate, alter, or drop; ShouldSend then decides per-recipient
// whether each resulting message actually goes out — this is what makes
// EquivocatingProposer (different block to different halves) and
// SilentNode (nothing to anyone) possible without consensus.State ever
// knowing it's being tested adversarially.
func (c *Cluster) dispatch(n *SimNode, msg consensus.Message) {
	behavior := n.getBehavior()

	var outMsgs []consensus.Message
	if msg.Proposal != nil {
		for _, p := range behavior.OnPropose(msg.Proposal) {
			outMsgs = append(outMsgs, consensus.Message{Proposal: p})
		}
	}
	if msg.Vote != nil {
		for _, v := range behavior.OnVote(msg.Vote) {
			outMsgs = append(outMsgs, consensus.Message{Vote: v})
		}
	}

	for _, out := range outMsgs {
		var payload any
		if out.Proposal != nil {
			payload = out.Proposal
		} else {
			payload = out.Vote
		}
		for _, target := range c.nodes {
			if target.Address == n.Address {
				continue // State already self-delivers; don't double-process.
			}
			if !behavior.ShouldSend(target.Address, payload) {
				continue
			}
			c.transport.deliver(n.Address, target.Address, out)
		}
	}
}

func (c *Cluster) pumpIn(n *SimNode) {
	for {
		select {
		case msg := <-n.inbox:
			if msg.Proposal != nil {
				_ = n.State.HandleProposal(msg.Proposal)
			}
			if msg.Vote != nil {
				_ = n.State.HandleVote(msg.Vote)
			}
		case <-c.stopCh:
			return
		}
	}
}

func (c *Cluster) pumpTimeout(n *SimNode) {
	for {
		select {
		case t := <-n.State.TimeoutChan():
			n.State.HandleTimeout(t)
		case <-c.stopCh:
			return
		}
	}
}

// WaitForHeight blocks until at least one node's store reaches height h,
// or returns an error once timeout elapses. "At least one" rather than
// "all" is deliberate: in Byzantine and partition scenarios, some nodes
// are expected to never reach a given height, and the claim under test
// is that the chain itself progressed, not that every node individually
// did — AssertNoFork separately verifies whatever every node *does* have
// agrees.
func (c *Cluster) WaitForHeight(h uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		for _, n := range c.nodes {
			if n.Store.Height() >= h {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("sim: timed out after %v waiting for height %d", timeout, h)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// AssertNoFork is the automated safety proof: for every height any node
// has committed, every other node that has also reached that height must
// agree on the exact same block hash. This is the assertion every
// scenario ends with, no exceptions.
func (c *Cluster) AssertNoFork() {
	c.t.Helper()

	var maxHeight uint64
	for _, n := range c.nodes {
		if h := n.Store.Height(); h > maxHeight {
			maxHeight = h
		}
	}

	for h := uint64(1); h <= maxHeight; h++ {
		var want []byte
		var wantNode int
		for i, n := range c.nodes {
			b, ok := n.Store.ByHeight(h)
			if !ok {
				continue // this node simply hasn't reached h yet — not a fork
			}
			if want == nil {
				want = b.Hash()
				wantNode = i
				continue
			}
			if !bytes.Equal(b.Hash(), want) {
				c.t.Fatalf("FORK at height %d: node %d has %x, node %d has %x",
					h, wantNode, want, i, b.Hash())
			}
		}
	}
}
