package validator

import (
	"testing"

	"github.com/ileon/bft-consensus/crypto"
)

func mustKeyPair(t *testing.T) (crypto.PrivKey, crypto.PubKey) {
	t.Helper()
	priv, pub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	return priv, pub
}

func makeValidators(t *testing.T, n int) []Validator {
	t.Helper()
	vals := make([]Validator, n)
	for i := 0; i < n; i++ {
		_, pub := mustKeyPair(t)
		vals[i] = Validator{
			Address:     pub.Address(),
			PubKey:      pub,
			VotingPower: 1,
		}
	}
	return vals
}

func TestNewSetOrderingIsDeterministic(t *testing.T) {
	vals := makeValidators(t, 5)

	forward, err := NewSet(vals)
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}

	reversed := make([]Validator, len(vals))
	for i, v := range vals {
		reversed[len(vals)-1-i] = v
	}
	backward, err := NewSet(reversed)
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}

	for i := 0; i < forward.Size(); i++ {
		if forward.validators[i].Address != backward.validators[i].Address {
			t.Fatalf("ordering diverged at index %d: %s vs %s",
				i, forward.validators[i].Address, backward.validators[i].Address)
		}
	}
}

func TestQuorumThresholdTable(t *testing.T) {
	cases := []struct {
		n    int
		want int64
	}{
		{1, 1},
		{4, 3},
		{7, 5},
		{10, 7},
		{100, 67},
	}
	for _, tc := range cases {
		vals := makeValidators(t, tc.n)
		set, err := NewSet(vals)
		if err != nil {
			t.Fatalf("NewSet: %v", err)
		}
		if got := set.QuorumThreshold(); got != tc.want {
			t.Errorf("N=%d: QuorumThreshold() = %d, want %d", tc.n, got, tc.want)
		}
	}
}

func TestProposerRotatesAndIsDeterministicAcrossSets(t *testing.T) {
	vals := makeValidators(t, 4)
	setA, err := NewSet(vals)
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	setB, err := NewSet(vals)
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}

	seen := map[crypto.Address]bool{}
	for r := uint32(0); r < uint32(len(vals)); r++ {
		pa := setA.Proposer(0, r)
		pb := setB.Proposer(0, r)
		if pa.Address != pb.Address {
			t.Fatalf("round %d: proposer diverged between independently constructed sets", r)
		}
		seen[pa.Address] = true
	}
	if len(seen) != len(vals) {
		t.Fatalf("expected proposer to rotate through all %d validators, saw %d", len(vals), len(seen))
	}
}

func TestNewSetRejectsDuplicateAddress(t *testing.T) {
	vals := makeValidators(t, 1)
	dup := append(vals, vals[0])
	if _, err := NewSet(dup); err != ErrDuplicateAddress {
		t.Fatalf("expected ErrDuplicateAddress, got %v", err)
	}
}

func TestNewSetRejectsEmpty(t *testing.T) {
	if _, err := NewSet(nil); err != ErrEmptySet {
		t.Fatalf("expected ErrEmptySet, got %v", err)
	}
}

func TestNewSetRejectsNonPositivePower(t *testing.T) {
	vals := makeValidators(t, 1)
	vals[0].VotingPower = 0
	if _, err := NewSet(vals); err != ErrNonPositivePower {
		t.Fatalf("expected ErrNonPositivePower, got %v", err)
	}
}

func TestGetByAddress(t *testing.T) {
	vals := makeValidators(t, 3)
	set, err := NewSet(vals)
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	v, ok := set.GetByAddress(vals[0].Address)
	if !ok {
		t.Fatal("expected to find validator by address")
	}
	if v.Address != vals[0].Address {
		t.Fatal("returned validator has wrong address")
	}
	if _, ok := set.GetByAddress(crypto.Address("nonexistent")); ok {
		t.Fatal("expected lookup of nonexistent address to fail")
	}
}

func TestHasQuorum(t *testing.T) {
	vals := makeValidators(t, 4)
	set, err := NewSet(vals)
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	if set.HasQuorum(2) {
		t.Fatal("2 of 4 should not be quorum")
	}
	if !set.HasQuorum(3) {
		t.Fatal("3 of 4 should be quorum")
	}
}
