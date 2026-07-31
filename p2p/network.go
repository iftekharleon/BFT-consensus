package p2p

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/ileon/bft-consensus/crypto"
)

// Transport is the interface consensus.State's driver broadcasts and
// sends through. Defining it separately from Network — the TCP
// implementation — means the simulation harness (sim/harness.go,
// landing in M7) can supply an in-memory implementation with no real
// sockets, making fault injection and reconnection-free deterministic
// tests trivial, while cmd/node uses the real one.
type Transport interface {
	Broadcast(e *Envelope)
	Send(to crypto.Address, e *Envelope)
	Receive() <-chan *Envelope
}

const (
	// handshakeLen is the fixed length of a crypto.Address in its
	// hex-encoded string form (20 bytes -> 40 hex chars), used as a
	// fixed-size, delimiter-free identity handshake immediately after a
	// connection is established.
	handshakeLen = 40

	outboundBufferSize = 64
	reconnectBaseDelay = 500 * time.Millisecond
	reconnectMaxDelay  = 30 * time.Second
)

// Peer tracks one validator's connection state: its outbound frame
// queue and, if this side ever dialed it, the address to redial on
// disconnect.
type Peer struct {
	address crypto.Address
	outCh   chan []byte

	mu       sync.Mutex
	conn     net.Conn // nil while disconnected
	dialAddr string   // empty if this peer was only ever Accepted, never Dialed
}

// Network is a fully-connected TCP mesh transport. At the validator
// counts this project targets (N <= 10) a full mesh is correct and
// simple; gossip only pays off at larger scale — see docs/design.md §7.
type Network struct {
	self       crypto.Address
	listenAddr string
	maxMsgSize int

	listener net.Listener

	mu    sync.RWMutex
	peers map[crypto.Address]*Peer

	inCh chan *Envelope

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewNetwork constructs a Network bound to listen on addr once Start is
// called, identifying itself to peers as self.
func NewNetwork(addr string, self crypto.Address) (*Network, error) {
	if addr == "" {
		return nil, fmt.Errorf("p2p: listen address must not be empty")
	}
	if self == "" {
		return nil, fmt.Errorf("p2p: self address must not be empty")
	}
	return &Network{
		self:       self,
		listenAddr: addr,
		maxMsgSize: DefaultMaxMsgSize,
		peers:      make(map[crypto.Address]*Peer),
		inCh:       make(chan *Envelope, 256),
		stopCh:     make(chan struct{}),
	}, nil
}

// SetMaxMsgSize overrides the default per-frame size limit enforced on
// reads, matching config.Config.MaxMsgSize. Must be called before Start.
func (n *Network) SetMaxMsgSize(size int) {
	n.maxMsgSize = size
}

// Start begins listening for inbound connections.
func (n *Network) Start() error {
	ln, err := net.Listen("tcp", n.listenAddr)
	if err != nil {
		return err
	}
	n.listener = ln
	n.wg.Add(1)
	go n.acceptLoop()
	return nil
}

func (n *Network) acceptLoop() {
	defer n.wg.Done()
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			select {
			case <-n.stopCh:
				return
			default:
				continue
			}
		}
		n.wg.Add(1)
		go n.handleAccepted(conn)
	}
}

func (n *Network) handleAccepted(conn net.Conn) {
	defer n.wg.Done()
	if err := writeHandshake(conn, n.self); err != nil {
		conn.Close()
		return
	}
	remote, err := readHandshake(conn)
	if err != nil {
		conn.Close()
		return
	}
	n.attachConn(remote, "", conn)
}

// Dial connects to addr, verifies the remote's identity matches peer via
// handshake, and registers it. On later disconnect, this address is
// used to reconnect automatically.
func (n *Network) Dial(addr string, peer crypto.Address) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	if err := writeHandshake(conn, n.self); err != nil {
		conn.Close()
		return err
	}
	remote, err := readHandshake(conn)
	if err != nil {
		conn.Close()
		return err
	}
	if remote != peer {
		conn.Close()
		return fmt.Errorf("p2p: handshake mismatch dialing %s: expected %s, got %s", addr, peer, remote)
	}
	n.attachConn(peer, addr, conn)
	return nil
}

func (n *Network) attachConn(addr crypto.Address, dialAddr string, conn net.Conn) {
	n.mu.Lock()
	p, exists := n.peers[addr]
	if !exists {
		p = &Peer{address: addr, outCh: make(chan []byte, outboundBufferSize)}
		n.peers[addr] = p
	}
	n.mu.Unlock()

	p.mu.Lock()
	if p.conn != nil {
		p.conn.Close()
	}
	p.conn = conn
	if dialAddr != "" {
		p.dialAddr = dialAddr
	}
	p.mu.Unlock()

	n.wg.Add(2)
	go n.readLoop(p, conn)
	go n.writeLoop(p, conn)
}

func (n *Network) readLoop(p *Peer, conn net.Conn) {
	defer n.wg.Done()
	defer n.handleDisconnect(p, conn)
	for {
		frame, err := readFrame(conn, n.maxMsgSize)
		if err != nil {
			return
		}
		env, err := Decode(frame)
		if err != nil {
			// One malformed frame doesn't justify killing an otherwise
			// live connection — drop it and keep reading.
			continue
		}
		select {
		case n.inCh <- env:
		case <-n.stopCh:
			return
		}
	}
}

func (n *Network) writeLoop(p *Peer, conn net.Conn) {
	defer n.wg.Done()
	for {
		select {
		case frame, ok := <-p.outCh:
			if !ok {
				return
			}
			if _, err := conn.Write(frame); err != nil {
				// Close rather than handling disconnect here directly:
				// this will unblock readLoop's Read with an error,
				// funneling all disconnect handling through one path.
				conn.Close()
				return
			}
		case <-n.stopCh:
			conn.Close()
			return
		}
	}
}

func (n *Network) handleDisconnect(p *Peer, conn net.Conn) {
	conn.Close()

	p.mu.Lock()
	isCurrent := p.conn == conn
	if isCurrent {
		p.conn = nil
	}
	dialAddr := p.dialAddr
	p.mu.Unlock()

	select {
	case <-n.stopCh:
		return
	default:
	}

	// Only reconnect if this was the peer's active connection (not a
	// stale one already superseded) and we know an address to redial.
	// A peer we only ever Accepted from has no known redial address;
	// without it, we wait for them to dial us again.
	if isCurrent && dialAddr != "" {
		n.wg.Add(1)
		go n.reconnectLoop(p, dialAddr)
	}
}

func (n *Network) reconnectLoop(p *Peer, dialAddr string) {
	defer n.wg.Done()
	delay := reconnectBaseDelay
	for {
		select {
		case <-n.stopCh:
			return
		case <-time.After(delay):
		}

		conn, err := net.Dial("tcp", dialAddr)
		if err == nil {
			if err := writeHandshake(conn, n.self); err == nil {
				if remote, err := readHandshake(conn); err == nil && remote == p.address {
					n.attachConn(p.address, dialAddr, conn)
					return
				}
			}
			conn.Close()
		}

		delay *= 2
		if delay > reconnectMaxDelay {
			delay = reconnectMaxDelay
		}
	}
}

// Broadcast sends e to every connected peer. Non-blocking per peer: a
// slow or dead peer's full outbound buffer causes that message to be
// dropped for that peer only, never blocking delivery to everyone else.
// Consensus tolerates message loss by design; it does not tolerate a
// blocked broadcast loop.
func (n *Network) Broadcast(e *Envelope) {
	frame, err := Encode(e)
	if err != nil {
		return
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, p := range n.peers {
		select {
		case p.outCh <- frame:
		default:
		}
	}
}

// Send delivers e to a single peer, non-blocking.
func (n *Network) Send(to crypto.Address, e *Envelope) {
	frame, err := Encode(e)
	if err != nil {
		return
	}
	n.mu.RLock()
	p, ok := n.peers[to]
	n.mu.RUnlock()
	if !ok {
		return
	}
	select {
	case p.outCh <- frame:
	default:
	}
}

// Receive is the channel of envelopes received from any peer.
func (n *Network) Receive() <-chan *Envelope {
	return n.inCh
}

// Stop closes the listener and all peer connections and waits for every
// goroutine to exit.
func (n *Network) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
	if n.listener != nil {
		n.listener.Close()
	}
	n.mu.RLock()
	for _, p := range n.peers {
		p.mu.Lock()
		if p.conn != nil {
			p.conn.Close()
		}
		p.mu.Unlock()
	}
	n.mu.RUnlock()
	n.wg.Wait()
}

func writeHandshake(conn net.Conn, self crypto.Address) error {
	_, err := conn.Write([]byte(self))
	return err
}

func readHandshake(conn net.Conn) (crypto.Address, error) {
	buf := make([]byte, handshakeLen)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return "", err
	}
	return crypto.Address(buf), nil
}

// readFrame reads one length-prefixed frame from conn, checking the
// claimed length against maxSize *before* allocating a buffer for the
// payload — an unchecked length prefix from an untrusted peer is a
// one-packet remote memory-exhaustion vector.
func readFrame(conn net.Conn, maxSize int) ([]byte, error) {
	var lenBuf [lengthPrefixSize]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if int(length) > maxSize {
		return nil, ErrOversizedFrame
	}
	frame := make([]byte, lengthPrefixSize+int(length))
	copy(frame[:lengthPrefixSize], lenBuf[:])
	if _, err := io.ReadFull(conn, frame[lengthPrefixSize:]); err != nil {
		return nil, err
	}
	return frame, nil
}
