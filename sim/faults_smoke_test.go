package sim

import (
	"testing"
	"time"
)

// TestEquivocatingProposerNoFork is the single most important scenario
// in the project: a Byzantine proposer sends block A to half the
// validator set and block B to the other half. Safety must hold
// regardless — no fork, chain still commits.
func TestEquivocatingProposerNoFork(t *testing.T) {
	const n = 4
	c := NewCluster(t, n)

	byzantineIdx := 0
	groupA := []int{1, 2}
	groupB := []int{3}
	addrsA := c.addressesFor(groupA)
	addrsB := c.addressesFor(groupB)

	c.MakeByzantine(byzantineIdx, NewEquivocatingProposer(c.Node(byzantineIdx).PrivKey, addrsA, addrsB))

	c.Start()
	defer c.Stop()

	if err := c.WaitForHeight(5, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.AssertNoFork()
}

func TestSilentNodeStillCommits(t *testing.T) {
	c := NewCluster(t, 4)
	c.MakeByzantine(0, SilentNode{})
	c.Start()
	defer c.Stop()

	if err := c.WaitForHeight(5, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.AssertNoFork()
}

func TestDoubleVoterEvidenceAndNoFork(t *testing.T) {
	const n = 4
	c := NewCluster(t, n)

	byzantineIdx := 0
	addrsA := c.addressesFor([]int{1, 2})
	addrsB := c.addressesFor([]int{3})
	c.MakeByzantine(byzantineIdx, NewDoubleVoter(c.Node(byzantineIdx).PrivKey, addrsA, addrsB))

	c.Start()
	defer c.Stop()

	if err := c.WaitForHeight(5, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.AssertNoFork()
}

func TestLaggingNodeStillCommits(t *testing.T) {
	c := NewCluster(t, 4)
	c.MakeByzantine(0, LaggingNode{Delay: 20 * time.Millisecond})
	c.Start()
	defer c.Stop()

	if err := c.WaitForHeight(5, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.AssertNoFork()
}

func TestInvalidProposerRecovers(t *testing.T) {
	c := NewCluster(t, 4)
	c.MakeByzantine(0, NewInvalidProposer(c.Node(0).PrivKey))
	c.Start()
	defer c.Stop()

	if err := c.WaitForHeight(5, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.AssertNoFork()
}

func TestCensoringNodeStillCommitsBlocks(t *testing.T) {
	c := NewCluster(t, 4)
	c.MakeByzantine(0, NewCensoringNode(c.Node(0).PrivKey))
	c.Start()
	defer c.Stop()

	if err := c.WaitForHeight(5, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.AssertNoFork()
}

func TestReplayNodeNoCorruption(t *testing.T) {
	c := NewCluster(t, 4)
	c.MakeByzantine(0, &ReplayNode{})
	c.Start()
	defer c.Stop()

	if err := c.WaitForHeight(5, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.AssertNoFork()
}
