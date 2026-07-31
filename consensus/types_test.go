package consensus

import (
	"bytes"
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

func TestProposalSignBytesIdenticalForEqualProposals(t *testing.T) {
	_, pub := mustKeyPair(t)
	blockHash := bytes.Repeat([]byte{0xAB}, 32)

	p1 := &Proposal{Height: 5, Round: 2, BlockHash: blockHash, POLRound: -1, Proposer: pub.Address()}
	p2 := &Proposal{Height: 5, Round: 2, BlockHash: append([]byte{}, blockHash...), POLRound: -1, Proposer: pub.Address()}

	if !bytes.Equal(p1.SignBytes(), p2.SignBytes()) {
		t.Fatal("expected sign bytes to be identical for logically equal proposals")
	}
}

func TestProposalSignBytesChangesWithEachField(t *testing.T) {
	_, pub := mustKeyPair(t)
	blockHash := bytes.Repeat([]byte{0xAB}, 32)
	base := &Proposal{Height: 5, Round: 2, BlockHash: blockHash, POLRound: -1, Proposer: pub.Address()}
	baseBytes := base.SignBytes()

	variants := []*Proposal{
		{Height: 6, Round: 2, BlockHash: blockHash, POLRound: -1, Proposer: pub.Address()},
		{Height: 5, Round: 3, BlockHash: blockHash, POLRound: -1, Proposer: pub.Address()},
		{Height: 5, Round: 2, BlockHash: bytes.Repeat([]byte{0xCD}, 32), POLRound: -1, Proposer: pub.Address()},
		{Height: 5, Round: 2, BlockHash: blockHash, POLRound: 1, Proposer: pub.Address()},
	}
	for i, v := range variants {
		if bytes.Equal(baseBytes, v.SignBytes()) {
			t.Errorf("variant %d: expected sign bytes to differ from base", i)
		}
	}
}

func TestProposalSignatureDoesNotVerifyAcrossHeights(t *testing.T) {
	priv, pub := mustKeyPair(t)
	blockHash := bytes.Repeat([]byte{0xAB}, 32)

	p := &Proposal{Height: 5, Round: 0, BlockHash: blockHash, POLRound: -1, Proposer: pub.Address()}
	p.Signature = priv.Sign(p.SignBytes())
	if !p.Verify(pub) {
		t.Fatal("expected original proposal to verify")
	}

	pNext := &Proposal{Height: 6, Round: 0, BlockHash: blockHash, POLRound: -1, Proposer: pub.Address()}
	pNext.Signature = p.Signature
	if pNext.Verify(pub) {
		t.Fatal("expected signature from height 5 to fail verification at height 6")
	}
}
