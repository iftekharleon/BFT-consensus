package p2p

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/ileon/bft-consensus/crypto"
)

func TestEncodeDecodeRoundTripsAllTypes(t *testing.T) {
	_, pub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}

	for _, typ := range []MsgType{MsgTypeProposal, MsgTypePrevote, MsgTypePrecommit} {
		e := &Envelope{Type: typ, From: pub.Address(), Payload: []byte("hello consensus")}
		frame, err := Encode(e)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		decoded, err := Decode(frame)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if decoded.Type != e.Type || decoded.From != e.From || !bytes.Equal(decoded.Payload, e.Payload) {
			t.Fatalf("round-trip mismatch: got %+v, want %+v", decoded, e)
		}
	}
}

func TestDecodeTruncatedFrameErrorsNoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Decode panicked on truncated frame: %v", r)
		}
	}()

	cases := [][]byte{
		nil,
		{},
		{0x00},
		{0x00, 0x00, 0x00},
		func() []byte {
			e := &Envelope{Type: MsgTypeProposal, From: "abcd", Payload: []byte("full payload")}
			frame, _ := Encode(e)
			return frame[:len(frame)-3] // chop off the tail
		}(),
	}
	for i, b := range cases {
		if _, err := Decode(b); err != ErrTruncatedFrame {
			t.Errorf("case %d: expected ErrTruncatedFrame, got %v", i, err)
		}
	}
}

func TestDecodeOversizedLengthPrefixRejectedBeforeAllocation(t *testing.T) {
	// A length prefix claiming ~4GB with no actual payload bytes
	// following. If Decode allocated or sliced based on the claimed
	// length before checking it, this would panic (slice out of range)
	// or attempt a huge allocation. It must instead reject immediately.
	b := make([]byte, lengthPrefixSize)
	binary.BigEndian.PutUint32(b, 0xFFFFFFFF)

	_, err := Decode(b)
	if err != ErrOversizedFrame {
		t.Fatalf("expected ErrOversizedFrame, got %v", err)
	}
}

func TestDecodeMalformedJSONReturnsError(t *testing.T) {
	frame := make([]byte, lengthPrefixSize+4)
	binary.BigEndian.PutUint32(frame[:lengthPrefixSize], 4)
	copy(frame[lengthPrefixSize:], []byte("nope")) // not valid JSON

	if _, err := Decode(frame); err == nil {
		t.Fatal("expected error decoding malformed JSON payload")
	}
}
