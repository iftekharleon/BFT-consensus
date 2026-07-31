package crypto

import "testing"

func TestSignVerifyRoundTrip(t *testing.T) {
	priv, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	msg := []byte("hello consensus")
	sig := priv.Sign(msg)
	if !pub.Verify(msg, sig) {
		t.Fatal("expected signature to verify")
	}
}

func TestVerifyRejectsTamperedMessage(t *testing.T) {
	priv, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	sig := priv.Sign([]byte("original"))
	if pub.Verify([]byte("tampered"), sig) {
		t.Fatal("expected verification of tampered message to fail")
	}
}

func TestVerifyRejectsWrongPubKey(t *testing.T) {
	priv, _, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	_, otherPub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	msg := []byte("hello consensus")
	sig := priv.Sign(msg)
	if otherPub.Verify(msg, sig) {
		t.Fatal("expected verification with wrong public key to fail")
	}
}

func TestPubKeyFromBytesRejectsMalformedInput(t *testing.T) {
	_, err := PubKeyFromBytes([]byte{0x01, 0x02, 0x03})
	if err == nil {
		t.Fatal("expected error for malformed key bytes, got nil")
	}
}

func TestPubKeyFromBytesRoundTrip(t *testing.T) {
	_, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	decoded, err := PubKeyFromBytes(pub)
	if err != nil {
		t.Fatalf("PubKeyFromBytes: %v", err)
	}
	if decoded.Address() != pub.Address() {
		t.Fatal("decoded pubkey address mismatch")
	}
}

func TestAddressIsStableAndTwentyBytes(t *testing.T) {
	_, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	addr1 := pub.Address()
	addr2 := pub.Address()
	if addr1 != addr2 {
		t.Fatal("expected Address() to be deterministic")
	}
	if len(addr1) != addressSize*2 { // hex-encoded
		t.Fatalf("expected %d hex chars, got %d", addressSize*2, len(addr1))
	}
}

func TestPrivKeyPublicMatchesGeneratedPubKey(t *testing.T) {
	priv, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	derived := priv.Public()
	if derived.Address() != pub.Address() {
		t.Fatal("expected PrivKey.Public() to match the pubkey returned by GenerateKeyPair")
	}
}
