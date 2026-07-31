package ledger

import (
	"bytes"
	"testing"

	"github.com/ileon/bft-consensus/crypto"
)

func testProposerAddr(t *testing.T) crypto.Address {
	t.Helper()
	_, pub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	return pub.Address()
}

func TestIdenticalHeadersHashIdentically(t *testing.T) {
	addr := testProposerAddr(t)
	parentHash := make([]byte, hashSize)

	h1 := Header{Height: 1, Timestamp: 100, ParentHash: parentHash, Proposer: addr}
	h2 := Header{Height: 1, Timestamp: 100, ParentHash: parentHash, Proposer: addr}

	b1 := NewBlock(h1, [][]byte{[]byte("tx1")})
	b2 := NewBlock(h2, [][]byte{[]byte("tx1")})

	if !bytes.Equal(b1.Hash(), b2.Hash()) {
		t.Fatal("expected identical headers to hash identically")
	}
}

func TestFieldChangeAltersHash(t *testing.T) {
	addr := testProposerAddr(t)
	parentHash := make([]byte, hashSize)

	base := NewBlock(Header{Height: 1, Timestamp: 100, ParentHash: parentHash, Proposer: addr}, nil)
	changedHeight := NewBlock(Header{Height: 2, Timestamp: 100, ParentHash: parentHash, Proposer: addr}, nil)
	changedTs := NewBlock(Header{Height: 1, Timestamp: 101, ParentHash: parentHash, Proposer: addr}, nil)

	if bytes.Equal(base.Hash(), changedHeight.Hash()) {
		t.Fatal("expected height change to alter hash")
	}
	if bytes.Equal(base.Hash(), changedTs.Hash()) {
		t.Fatal("expected timestamp change to alter hash")
	}
}

func TestNilAndEmptyTxsProduceSameTxRoot(t *testing.T) {
	nilRoot := ComputeTxRoot(nil)
	emptyRoot := ComputeTxRoot([][]byte{})
	if !bytes.Equal(nilRoot, emptyRoot) {
		t.Fatal("expected nil and empty tx slices to produce the same TxRoot")
	}
	if len(nilRoot) != hashSize {
		t.Fatalf("expected empty TxRoot to be %d zero bytes, got %d bytes", hashSize, len(nilRoot))
	}
	for _, b := range nilRoot {
		if b != 0 {
			t.Fatal("expected empty TxRoot to be all zero bytes")
		}
	}
}

func TestValidateBasic(t *testing.T) {
	addr := testProposerAddr(t)
	parentHash := make([]byte, hashSize)

	valid := NewBlock(Header{Height: 1, Timestamp: 100, ParentHash: parentHash, Proposer: addr}, nil)
	if err := valid.ValidateBasic(); err != nil {
		t.Fatalf("expected valid block to pass ValidateBasic, got %v", err)
	}

	zeroHeight := NewBlock(Header{Height: 0, Timestamp: 100, ParentHash: parentHash, Proposer: addr}, nil)
	if err := zeroHeight.ValidateBasic(); err != ErrInvalidHeight {
		t.Fatalf("expected ErrInvalidHeight, got %v", err)
	}

	badParentHash := NewBlock(Header{Height: 1, Timestamp: 100, ParentHash: []byte{0x01}, Proposer: addr}, nil)
	if err := badParentHash.ValidateBasic(); err != ErrInvalidParentHashLength {
		t.Fatalf("expected ErrInvalidParentHashLength, got %v", err)
	}

	badProposer := NewBlock(Header{Height: 1, Timestamp: 100, ParentHash: parentHash, Proposer: crypto.Address("not-hex")}, nil)
	if err := badProposer.ValidateBasic(); err != ErrInvalidProposerAddress {
		t.Fatalf("expected ErrInvalidProposerAddress, got %v", err)
	}
}

func TestValidateAgainstParent(t *testing.T) {
	addr := testProposerAddr(t)
	genesisParentHash := make([]byte, hashSize)
	parent := NewBlock(Header{Height: 1, Timestamp: 100, ParentHash: genesisParentHash, Proposer: addr}, nil)

	valid := NewBlock(Header{Height: 2, Timestamp: 101, ParentHash: parent.Hash(), Proposer: addr}, nil)
	if err := valid.ValidateAgainstParent(parent); err != nil {
		t.Fatalf("expected valid child to pass, got %v", err)
	}

	heightGap := NewBlock(Header{Height: 3, Timestamp: 101, ParentHash: parent.Hash(), Proposer: addr}, nil)
	if err := heightGap.ValidateAgainstParent(parent); err != ErrHeightMismatch {
		t.Fatalf("expected ErrHeightMismatch, got %v", err)
	}

	wrongParentHash := NewBlock(Header{Height: 2, Timestamp: 101, ParentHash: genesisParentHash, Proposer: addr}, nil)
	if err := wrongParentHash.ValidateAgainstParent(parent); err != ErrParentHashMismatch {
		t.Fatalf("expected ErrParentHashMismatch, got %v", err)
	}

	staleTimestamp := NewBlock(Header{Height: 2, Timestamp: 100, ParentHash: parent.Hash(), Proposer: addr}, nil)
	if err := staleTimestamp.ValidateAgainstParent(parent); err != ErrTimestampNotIncreasing {
		t.Fatalf("expected ErrTimestampNotIncreasing, got %v", err)
	}
}
