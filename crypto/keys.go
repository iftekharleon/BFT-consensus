// Package crypto provides ed25519-based validator identity: keygen, sign,
// verify, and address derivation. See PROJECT_SPEC.md §3.1.
//
// ed25519 is used instead of ECDSA because ed25519 signatures are
// deterministic by construction — no nonce generation is needed at sign
// time, which removes an entire class of nonce-reuse key-recovery bugs that
// have historically plagued ECDSA implementations (e.g. reused-k attacks).
package crypto

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

// PrivKey is a validator's ed25519 private key.
type PrivKey ed25519.PrivateKey

// PubKey is a validator's ed25519 public key.
type PubKey ed25519.PublicKey

// Address is the hex-encoded, truncated hash of a validator's public key.
type Address string

var (
	// ErrInvalidPubKeyLength is returned when decoding a public key whose
	// byte length does not match ed25519.PublicKeySize.
	ErrInvalidPubKeyLength = errors.New("crypto: invalid public key length")
)

// addressSize is 20 bytes to match Ethereum's address convention, which
// keeps logs and CLI output readable without truncating a full 32-byte hash.
const addressSize = 20

// GenerateKeyPair creates a new ed25519 keypair.
func GenerateKeyPair() (PrivKey, PubKey, error) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, err
	}
	return PrivKey(priv), PubKey(pub), nil
}

// Sign signs msg with the private key. ed25519 signing is deterministic, so
// no nonce handling is required.
func (p PrivKey) Sign(msg []byte) []byte {
	return ed25519.Sign(ed25519.PrivateKey(p), msg)
}

// Verify reports whether sig is a valid signature of msg by the public key.
func (p PubKey) Verify(msg, sig []byte) bool {
	if len(p) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(p), msg, sig)
}

// Address derives this public key's address as hex(sha256(pubkey)[:20]).
func (p PubKey) Address() Address {
	sum := sha256.Sum256(p)
	return Address(hex.EncodeToString(sum[:addressSize]))
}

// PubKeyFromBytes decodes a public key from raw bytes. It returns an error
// rather than panicking on malformed input — this function sits on a
// network-facing path (deserializing peer-supplied keys), and a panic there
// would be a trivial remote DoS.
func PubKeyFromBytes(b []byte) (PubKey, error) {
	if len(b) != ed25519.PublicKeySize {
		return nil, ErrInvalidPubKeyLength
	}
	pub := make(PubKey, ed25519.PublicKeySize)
	copy(pub, b)
	return pub, nil
}
