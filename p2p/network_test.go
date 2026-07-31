package p2p

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/ileon/bft-consensus/crypto"
)

func mustNetwork(t *testing.T, addr string) (*Network, crypto.Address) {
	t.Helper()
	_, pub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	n, err := NewNetwork(addr, pub.Address())
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(n.Stop)
	return n, pub.Address()
}

func waitForEnvelope(t *testing.T, ch <-chan *Envelope, timeout time.Duration) *Envelope {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(timeout):
		t.Fatal("timed out waiting for envelope")
		return nil
	}
}

func TestTwoNodesConnectAndExchangeMessages(t *testing.T) {
	a, aAddr := mustNetwork(t, "127.0.0.1:0")
	b, bAddr := mustNetwork(t, "127.0.0.1:0")

	if err := a.Dial(b.listener.Addr().String(), bAddr); err != nil {
		t.Fatalf("Dial: %v", err)
	}

	// Give b's accept handler a moment to register a's connection too
	// (the handshake completes asynchronously on b's side).
	time.Sleep(50 * time.Millisecond)

	a.Send(bAddr, &Envelope{Type: MsgTypeProposal, From: aAddr, Payload: []byte("hello from a")})
	got := waitForEnvelope(t, b.Receive(), 2*time.Second)
	if got.From != aAddr || string(got.Payload) != "hello from a" {
		t.Fatalf("unexpected envelope at b: %+v", got)
	}

	b.Send(aAddr, &Envelope{Type: MsgTypePrevote, From: bAddr, Payload: []byte("hello from b")})
	got = waitForEnvelope(t, a.Receive(), 2*time.Second)
	if got.From != bAddr || string(got.Payload) != "hello from b" {
		t.Fatalf("unexpected envelope at a: %+v", got)
	}
}

func TestKillingPeerDoesNotCrashSurvivor(t *testing.T) {
	a, _ := mustNetwork(t, "127.0.0.1:0")
	b, bAddr := mustNetwork(t, "127.0.0.1:0")

	if err := a.Dial(b.listener.Addr().String(), bAddr); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	b.Stop() // kill b abruptly

	// a must survive: Broadcast/Send after the peer's death must not
	// panic or block.
	done := make(chan struct{})
	go func() {
		a.Broadcast(&Envelope{Type: MsgTypeProposal, From: "", Payload: []byte("still alive")})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast blocked after peer death")
	}
}

func TestReconnectSucceedsAfterPeerRestart(t *testing.T) {
	a, _ := mustNetwork(t, "127.0.0.1:0")

	_, bPub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	bAddr := bPub.Address()

	b1, err := NewNetwork("127.0.0.1:0", bAddr)
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}
	if err := b1.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	bListenAddr := b1.listener.Addr().String()

	if err := a.Dial(bListenAddr, bAddr); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	b1.Stop() // simulate b crashing

	// Restart b on the SAME address.
	b2, err := NewNetwork(bListenAddr, bAddr)
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}
	if err := b2.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(b2.Stop)

	// a's reconnect loop (base delay 500ms) should re-establish the
	// connection; confirm by sending a message through once reconnected.
	deadline := time.Now().Add(10 * time.Second)
	for {
		a.Send(bAddr, &Envelope{Type: MsgTypeProposal, From: "", Payload: []byte("post-restart")})
		select {
		case got := <-b2.Receive():
			if string(got.Payload) != "post-restart" {
				t.Fatalf("unexpected payload after reconnect: %s", got.Payload)
			}
			return
		case <-time.After(300 * time.Millisecond):
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for reconnect after peer restart")
			}
		}
	}
}

func TestSlowPeerDoesNotBlockBroadcastToOthers(t *testing.T) {
	a, _ := mustNetwork(t, "127.0.0.1:0")
	fast, fastAddr := mustNetwork(t, "127.0.0.1:0")

	// "Slow" peer: accepts the connection but never reads, so its TCP
	// receive buffer (and then a's outbound buffer) eventually fills.
	_, slowPub, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	slowAddr := slowPub.Address()
	slow, err := NewNetwork("127.0.0.1:0", slowAddr)
	if err != nil {
		t.Fatalf("NewNetwork: %v", err)
	}
	if err := slow.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(slow.Stop)

	if err := a.Dial(fast.listener.Addr().String(), fastAddr); err != nil {
		t.Fatalf("Dial fast: %v", err)
	}
	if err := a.Dial(slow.listener.Addr().String(), slowAddr); err != nil {
		t.Fatalf("Dial slow: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Stall the slow peer's reader deliberately by not calling Receive()
	// on it at all (already the case) and flood it past its outbound
	// buffer capacity from a's side.
	done := make(chan struct{})
	go func() {
		for i := 0; i < outboundBufferSize*4; i++ {
			a.Broadcast(&Envelope{Type: MsgTypeProposal, From: "", Payload: []byte("flood")})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Broadcast blocked due to one slow peer")
	}

	// The fast peer must still receive messages despite the slow one.
	waitForEnvelope(t, fast.Receive(), 2*time.Second)
}

// TestReadFrameReassemblesFragmentedWrites proves readFrame correctly
// reassembles a frame delivered across multiple separate Write calls —
// TCP is a byte stream, not a message stream, so a reader that assumes
// one Write equals one logical frame will get partial reads and
// corrupted decodes the moment a frame straddles two TCP segments.
func TestReadFrameReassemblesFragmentedWrites(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	e := &Envelope{Type: MsgTypeProposal, From: "abcd1234", Payload: []byte("a fragmented payload")}
	frame, err := Encode(e)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(frame) < 4 {
		t.Fatalf("frame too short to fragment meaningfully: %d bytes", len(frame))
	}
	split := len(frame) / 2

	go func() {
		client.Write(frame[:split])
		time.Sleep(20 * time.Millisecond)
		client.Write(frame[split:])
	}()

	got, err := readFrame(server, DefaultMaxMsgSize)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if !bytes.Equal(got, frame) {
		t.Fatal("reassembled frame does not match original")
	}

	decoded, err := Decode(got)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.From != e.From || string(decoded.Payload) != string(e.Payload) {
		t.Fatal("decoded envelope does not match original after fragmented reassembly")
	}
}
