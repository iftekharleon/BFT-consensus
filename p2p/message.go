// Package p2p wraps consensus messages in a typed, length-prefixed wire
// envelope and provides a Transport interface with a fully-connected TCP
// mesh implementation. See PROJECT_SPEC.md §3.11-3.12.
package p2p

import (
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/ileon/bft-consensus/crypto"
)

// MsgType identifies the kind of consensus message an Envelope carries.
type MsgType uint8

const (
	MsgTypeProposal MsgType = iota + 1
	MsgTypePrevote
	MsgTypePrecommit
)

// Envelope wraps a consensus message for transmission. Payload holds the
// JSON-encoded Proposal or Vote; the consumer dispatches on Type to know
// which to unmarshal it as.
type Envelope struct {
	Type    MsgType
	From    crypto.Address
	Payload []byte
}

const (
	lengthPrefixSize = 4

	// DefaultMaxMsgSize bounds a single envelope's total encoded size
	// for Decode's own built-in safety net (used directly by tests and
	// FuzzDecode). Network-level enforcement uses the configured
	// config.Config.MaxMsgSize instead, checked by the caller before
	// Decode is ever invoked with attacker-supplied bytes — see
	// network.go.
	DefaultMaxMsgSize = 1 << 20 // 1 MiB
)

var (
	// ErrTruncatedFrame is returned when b does not contain a complete
	// frame: either fewer than 4 bytes (no length prefix) or fewer
	// bytes than the length prefix claims.
	ErrTruncatedFrame = errors.New("p2p: truncated frame")

	// ErrOversizedFrame is returned when the length prefix exceeds
	// DefaultMaxMsgSize.
	ErrOversizedFrame = errors.New("p2p: frame exceeds maximum message size")
)

// Encode serializes e into a length-prefixed frame: a 4-byte big-endian
// length followed by the JSON-encoded envelope. TCP is a byte stream,
// not a message stream — without an explicit length prefix, a reader
// has no way to know where one envelope ends and the next begins, and
// will get partial reads and corrupted decodes.
func Encode(e *Envelope) ([]byte, error) {
	payload, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, lengthPrefixSize+len(payload))
	binary.BigEndian.PutUint32(frame[:lengthPrefixSize], uint32(len(payload)))
	copy(frame[lengthPrefixSize:], payload)
	return frame, nil
}

// Decode parses a length-prefixed frame back into an Envelope. It never
// panics on malformed input — b may be truncated, carry a garbage length
// prefix, or contain invalid JSON, and each of those returns a plain
// error rather than crashing the process. The length is validated
// against DefaultMaxMsgSize before any payload-sized slicing or
// unmarshaling proceeds: a length-prefix parser that processes the
// claimed size before checking it is a one-packet remote resource-
// exhaustion vector.
func Decode(b []byte) (*Envelope, error) {
	if len(b) < lengthPrefixSize {
		return nil, ErrTruncatedFrame
	}
	length := binary.BigEndian.Uint32(b[:lengthPrefixSize])
	if length > DefaultMaxMsgSize {
		return nil, ErrOversizedFrame
	}
	if uint32(len(b)-lengthPrefixSize) < length {
		return nil, ErrTruncatedFrame
	}

	payload := b[lengthPrefixSize : lengthPrefixSize+int(length)]
	var e Envelope
	if err := json.Unmarshal(payload, &e); err != nil {
		return nil, err
	}
	return &e, nil
}
