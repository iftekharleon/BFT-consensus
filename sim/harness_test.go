package sim

import (
	"testing"
	"time"

	"github.com/ileon/bft-consensus/ledger"
)

func TestFourNodeAllHonestReachesHeightTenFast(t *testing.T) {
	c := NewCluster(t, 4)
	c.Start()
	defer c.Stop()

	start := time.Now()
	if err := c.WaitForHeight(10, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("expected height 10 within a second, took %v", elapsed)
	}
	c.AssertNoFork()
}

func TestPartitionAndHealTakeEffectImmediately(t *testing.T) {
	c := NewCluster(t, 4)
	c.Start()
	defer c.Stop()

	if err := c.WaitForHeight(1, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	// 2-2 split: neither side has quorum (threshold 3 of 4), so the
	// chain must halt.
	c.Partition([]int{0, 1}, []int{2, 3})
	heightAtPartition := c.Node(0).Store.Height()
	time.Sleep(200 * time.Millisecond)
	if h := c.Node(0).Store.Height(); h > heightAtPartition+1 {
		t.Fatalf("expected no meaningful progress during 2-2 partition, height moved from %d to %d", heightAtPartition, h)
	}

	c.HealPartition()
	if err := c.WaitForHeight(heightAtPartition+3, 2*time.Second); err != nil {
		t.Fatalf("expected progress to resume promptly after healing: %v", err)
	}
	c.AssertNoFork()
}

// TestAssertNoForkCatchesDeliberateFork verifies the safety assertion
// itself: handed two nodes with genuinely divergent committed blocks at
// the same height, it must fail loudly, not pass silently.
func TestAssertNoForkCatchesDeliberateFork(t *testing.T) {
	c := NewCluster(t, 2)

	genesis := c.Node(0).Store.Head()
	blockA := ledger.NewBlock(ledger.Header{
		Height: 1, Timestamp: 100, ParentHash: genesis.Hash(), Proposer: c.Node(0).Address,
	}, nil)
	blockB := ledger.NewBlock(ledger.Header{
		Height: 1, Timestamp: 200, ParentHash: genesis.Hash(), Proposer: c.Node(1).Address,
	}, nil)
	if err := c.Node(0).Store.Append(blockA); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := c.Node(1).Store.Append(blockB); err != nil {
		t.Fatalf("Append: %v", err)
	}

	fakeT := &testing.T{}
	forked := &Cluster{t: fakeT, nodes: c.nodes}
	if !runAssertNoForkExpectingFailure(forked) {
		t.Fatal("expected AssertNoFork to detect the deliberate fork")
	}
}

// runAssertNoForkExpectingFailure runs AssertNoFork in a subprocess-free
// way by checking Failed() on the throwaway *testing.T passed to the
// forked cluster, since AssertNoFork calls t.Fatalf rather than
// returning an error (matching the spec's exact API).
func runAssertNoForkExpectingFailure(c *Cluster) (failed bool) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { recover() }() // t.Fatalf calls runtime.Goexit via FailNow; recover isn't reached, but guard anyway
		c.AssertNoFork()
	}()
	<-done
	return c.t.Failed()
}
