package consensus

import (
	"bytes"
	"testing"
)

func TestPrevoteSignatureDoesNotVerifyAsPrecommit(t *testing.T) {
	priv, pub := mustKeyPair(t)
	blockHash := bytes.Repeat([]byte{0xAB}, 32)

	prevote := &Vote{Type: VoteTypePrevote, Height: 1, Round: 0, BlockHash: blockHash, Voter: pub.Address(), ChainID: "test"}
	prevote.Signature = priv.Sign(prevote.SignBytes())
	if !prevote.Verify(pub) {
		t.Fatal("expected prevote to verify")
	}

	precommit := &Vote{Type: VoteTypePrecommit, Height: 1, Round: 0, BlockHash: blockHash, Voter: pub.Address(), ChainID: "test"}
	precommit.Signature = prevote.Signature
	if precommit.Verify(pub) {
		t.Fatal("expected prevote signature to fail verification as a precommit")
	}
}

func TestNilVoteRoundTrips(t *testing.T) {
	priv, pub := mustKeyPair(t)
	v := &Vote{Type: VoteTypePrevote, Height: 1, Round: 0, BlockHash: nil, Voter: pub.Address(), ChainID: "test"}
	v.Signature = priv.Sign(v.SignBytes())

	if !v.IsNil() {
		t.Fatal("expected vote with nil BlockHash to be NIL")
	}
	if !v.Verify(pub) {
		t.Fatal("expected NIL vote to verify")
	}
}

func TestSameVoteDifferentRoundProducesDifferentSignBytes(t *testing.T) {
	_, pub := mustKeyPair(t)
	blockHash := bytes.Repeat([]byte{0xAB}, 32)

	v1 := &Vote{Type: VoteTypePrevote, Height: 1, Round: 0, BlockHash: blockHash, Voter: pub.Address(), ChainID: "test"}
	v2 := &Vote{Type: VoteTypePrevote, Height: 1, Round: 1, BlockHash: blockHash, Voter: pub.Address(), ChainID: "test"}

	if bytes.Equal(v1.SignBytes(), v2.SignBytes()) {
		t.Fatal("expected different rounds to produce different sign bytes")
	}
}

func TestVoteTypeZeroValueIsInvalid(t *testing.T) {
	var zero VoteType
	if zero == VoteTypePrevote || zero == VoteTypePrecommit {
		t.Fatal("expected zero value VoteType to be neither Prevote nor Precommit")
	}
}
