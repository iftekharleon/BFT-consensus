package consensus

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ileon/bft-consensus/crypto"
	"github.com/ileon/bft-consensus/validator"
)

type testVal struct {
	priv crypto.PrivKey
	pub  crypto.PubKey
	addr crypto.Address
}

func makeTestValidatorSet(t *testing.T, n int) (*validator.Set, []testVal) {
	t.Helper()
	tvs := make([]testVal, n)
	vals := make([]validator.Validator, n)
	for i := 0; i < n; i++ {
		priv, pub := mustKeyPair(t)
		tvs[i] = testVal{priv: priv, pub: pub, addr: pub.Address()}
		vals[i] = validator.Validator{Address: pub.Address(), PubKey: pub, VotingPower: 1}
	}
	set, err := validator.NewSet(vals)
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	return set, tvs
}

func signedVote(tv testVal, height uint64, round uint32, vt VoteType, blockHash []byte) *Vote {
	v := &Vote{
		Type:      vt,
		Height:    height,
		Round:     round,
		BlockHash: blockHash,
		Voter:     tv.addr,
		ChainID:   "test-chain",
	}
	v.Signature = tv.priv.Sign(v.SignBytes())
	return v
}

func TestMajorityDetectedAtExactlyThirdVote(t *testing.T) {
	set, tvs := makeTestValidatorSet(t, 4)
	vs := NewVoteSet(1, 0, VoteTypePrevote, set)
	hash := bytes.Repeat([]byte{0xAA}, 32)

	for i := 0; i < 2; i++ {
		if _, err := vs.AddVote(signedVote(tvs[i], 1, 0, VoteTypePrevote, hash)); err != nil {
			t.Fatalf("AddVote %d: %v", i, err)
		}
		if _, ok := vs.HasTwoThirdsMajority(); ok {
			t.Fatalf("expected no majority after %d votes", i+1)
		}
	}

	if _, err := vs.AddVote(signedVote(tvs[2], 1, 0, VoteTypePrevote, hash)); err != nil {
		t.Fatalf("AddVote 2: %v", err)
	}
	got, ok := vs.HasTwoThirdsMajority()
	if !ok {
		t.Fatal("expected majority after 3rd vote")
	}
	if !bytes.Equal(got, hash) {
		t.Fatal("majority hash mismatch")
	}
}

func TestDuplicateIdenticalVoteNoOp(t *testing.T) {
	set, tvs := makeTestValidatorSet(t, 4)
	vs := NewVoteSet(1, 0, VoteTypePrevote, set)
	hash := bytes.Repeat([]byte{0xAA}, 32)

	v := signedVote(tvs[0], 1, 0, VoteTypePrevote, hash)
	if added, err := vs.AddVote(v); !added || err != nil {
		t.Fatalf("first AddVote: added=%v err=%v", added, err)
	}
	powerBefore := vs.TotalPower()

	added, err := vs.AddVote(v)
	if added {
		t.Fatal("expected duplicate identical vote to not be added")
	}
	if err != nil {
		t.Fatalf("expected no error for duplicate identical vote, got %v", err)
	}
	if vs.TotalPower() != powerBefore {
		t.Fatal("expected power to be unchanged after duplicate vote")
	}
}

func TestConflictingVoteProducesEquivocationEvidence(t *testing.T) {
	set, tvs := makeTestValidatorSet(t, 4)
	vs := NewVoteSet(1, 0, VoteTypePrevote, set)
	hashA := bytes.Repeat([]byte{0xAA}, 32)
	hashB := bytes.Repeat([]byte{0xBB}, 32)

	voteA := signedVote(tvs[0], 1, 0, VoteTypePrevote, hashA)
	if _, err := vs.AddVote(voteA); err != nil {
		t.Fatalf("AddVote A: %v", err)
	}

	voteB := signedVote(tvs[0], 1, 0, VoteTypePrevote, hashB)
	added, err := vs.AddVote(voteB)
	if added {
		t.Fatal("expected equivocating vote to not be added")
	}
	if !errors.Is(err, ErrEquivocation) {
		t.Fatalf("expected ErrEquivocation, got %v", err)
	}
	var eqErr *EquivocationError
	if !errors.As(err, &eqErr) {
		t.Fatalf("expected *EquivocationError, got %T", err)
	}
	if eqErr.Evidence.VoteA != voteA || eqErr.Evidence.VoteB != voteB {
		t.Fatal("expected both conflicting votes retrievable from Evidence")
	}
}

func TestVoteFromNonMemberRejected(t *testing.T) {
	set, _ := makeTestValidatorSet(t, 4)
	vs := NewVoteSet(1, 0, VoteTypePrevote, set)

	outsiderPriv, outsiderPub := mustKeyPair(t)
	v := &Vote{Type: VoteTypePrevote, Height: 1, Round: 0, BlockHash: nil, Voter: outsiderPub.Address(), ChainID: "test-chain"}
	v.Signature = outsiderPriv.Sign(v.SignBytes())

	added, err := vs.AddVote(v)
	if added {
		t.Fatal("expected vote from non-member to be rejected")
	}
	if !errors.Is(err, ErrNotAValidator) {
		t.Fatalf("expected ErrNotAValidator, got %v", err)
	}
}

func TestSplitVotesHasTwoThirdsAny(t *testing.T) {
	set, tvs := makeTestValidatorSet(t, 4)
	vs := NewVoteSet(1, 0, VoteTypePrevote, set)
	hashA := bytes.Repeat([]byte{0xAA}, 32)
	hashB := bytes.Repeat([]byte{0xBB}, 32)

	if _, err := vs.AddVote(signedVote(tvs[0], 1, 0, VoteTypePrevote, hashA)); err != nil {
		t.Fatalf("AddVote: %v", err)
	}
	if _, err := vs.AddVote(signedVote(tvs[1], 1, 0, VoteTypePrevote, hashB)); err != nil {
		t.Fatalf("AddVote: %v", err)
	}
	if vs.HasTwoThirdsAny() {
		t.Fatal("expected HasTwoThirdsAny false with only 2 of 4 votes cast")
	}

	if _, err := vs.AddVote(signedVote(tvs[2], 1, 0, VoteTypePrevote, hashA)); err != nil {
		t.Fatalf("AddVote: %v", err)
	}
	if !vs.HasTwoThirdsAny() {
		t.Fatal("expected HasTwoThirdsAny true with 3 of 4 votes cast (split 2/1)")
	}
	if _, ok := vs.HasTwoThirdsMajority(); ok {
		t.Fatal("expected no majority when votes are split 2/1 (2 < quorum of 3)")
	}
}
