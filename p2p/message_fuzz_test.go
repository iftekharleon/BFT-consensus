package p2p

import "testing"

// FuzzDecode exercises Decode against arbitrary byte slices. Decode must
// never panic, regardless of how malformed or adversarial the input is —
// this is the untrusted-network boundary of the whole project.
func FuzzDecode(f *testing.F) {
	valid, _ := Encode(&Envelope{Type: MsgTypeProposal, From: "abcd1234", Payload: []byte("seed payload")})
	f.Add(valid)
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	f.Add([]byte{0x00, 0x00, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o'})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Decode(data)
	})
}
