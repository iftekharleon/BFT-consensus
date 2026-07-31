package sim

import (
	"sync"

	"github.com/ileon/bft-consensus/consensus"
	"github.com/ileon/bft-consensus/crypto"
)

// memTransport routes consensus messages between in-process nodes via
// channels — no real sockets. It owns the partition table that
// Cluster.Partition/HealPartition manipulate, making that the injection
// point for network-split scenarios exactly as the per-node Behavior
// hook is the injection point for Byzantine content.
type memTransport struct {
	mu    sync.RWMutex
	inbox map[crypto.Address]chan consensus.Message

	partitioned bool
	groupA      map[crypto.Address]bool
	groupB      map[crypto.Address]bool
}

func newMemTransport() *memTransport {
	return &memTransport{inbox: make(map[crypto.Address]chan consensus.Message)}
}

// register creates and returns the inbound channel for addr.
func (mt *memTransport) register(addr crypto.Address) chan consensus.Message {
	ch := make(chan consensus.Message, 256)
	mt.mu.Lock()
	mt.inbox[addr] = ch
	mt.mu.Unlock()
	return ch
}

// deliver sends msg from -> to, non-blocking, respecting the current
// partition state. A full inbox drops the message rather than blocking
// the sender — consensus tolerates message loss by design.
func (mt *memTransport) deliver(from, to crypto.Address, msg consensus.Message) {
	mt.mu.RLock()
	reachable := !mt.partitioned || (mt.groupA[from] && mt.groupA[to]) || (mt.groupB[from] && mt.groupB[to])
	ch, ok := mt.inbox[to]
	mt.mu.RUnlock()

	if !reachable || !ok {
		return
	}
	select {
	case ch <- msg:
	default:
	}
}

func (mt *memTransport) partition(groupA, groupB []crypto.Address) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.partitioned = true
	mt.groupA = toSet(groupA)
	mt.groupB = toSet(groupB)
}

func (mt *memTransport) heal() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.partitioned = false
	mt.groupA = nil
	mt.groupB = nil
}

func toSet(addrs []crypto.Address) map[crypto.Address]bool {
	s := make(map[crypto.Address]bool, len(addrs))
	for _, a := range addrs {
		s[a] = true
	}
	return s
}
