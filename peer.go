package bonfire

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	nat "github.com/mediocregopher/go-nat"
)

// PeerOpts are passed to the NewPeer function to affect the Peer's behavior.
type PeerOpts struct {
	// When sending a packet to the server or a peer, determines how many times
	// the packet is sent (in case any are dropped). Default is 3.
	PacketBlastCount int

	// The time NewPeer will wait for HelloPeer messages from other peers before
	// attempting to communicate with a potential NAT gateway to open an
	// external port. Default is 1 * time.Second.
	//
	// If -1, this timeout is ignored and NAT gateway port forwarding is never
	// attempted.
	InitTimeoutUntilGateway time.Duration

	// When a port mapping is created on a NAT gateway for this peer, this
	// timeout will be used as the expiration for that mapping on the gateway
	// and to determine how often to refresh that mapping (so it doesn't expire
	// while the peer is active). Default is 1 * time.Minute.
	GatewayPortMapTimeout time.Duration

	// The interval on which ReadyToMingle messages are sent. If -1, no
	// ReadyToMingle messages will be sent. Default is 1 * time.Minute.
	ReadyToMingleInterval time.Duration

	// Address to listen on when creating the UDP port. Default is ":0", which
	// means any IP address over a randomly picked port.
	ListenAddr string

	// MaxPeers indicates the maximum number of peers to keep track of (i.e.,
	// maximum number which will be returned from PeerAddrs). Default is 10.
	MaxPeers int

	// FingerprintFunc can be used to generate the Message fingerprints used by
	// the Peer. A fingerprint must be exactly FingerprintSize bytes. See
	// Server's FingerprintCheck field for an example of how this might be used.
	FingerprintFunc func() ([]byte, error)
}

func (po PeerOpts) withDefaults() PeerOpts {
	if po.PacketBlastCount == 0 {
		po.PacketBlastCount = 3
	}
	if po.InitTimeoutUntilGateway == 0 {
		po.InitTimeoutUntilGateway = 1 * time.Second
	}
	if po.GatewayPortMapTimeout == 0 {
		po.GatewayPortMapTimeout = 1 * time.Minute
	}
	if po.ReadyToMingleInterval == 0 {
		po.ReadyToMingleInterval = 1 * time.Minute
	}
	if po.ListenAddr == "" {
		po.ListenAddr = ":0"
	}
	if po.MaxPeers == 0 {
		po.MaxPeers = 10
	}
	return po
}

// Peer implements a bonfire peer which can discover other peers from a bonfire
// server and multiplex bonfire and application packets over a single UDP
// port.
//
// Until Close is called the Peer will hold open the socket it creates to talk
// with the server, and accepts packets from peers over that socket as well (see
// ReadFrom method).
//
// Until Close is called the Peer will periodically send ReadyToMingle
// messages (unless the interval is 0 in PeerOpts) to the server so that it
// can help new peers discover itself.
type Peer struct {
	// Peer wraps a PacketConn, overwriting some of its methods and exposing the
	// rest.
	net.PacketConn

	po                     PeerOpts
	network, serverAddrStr string
	gw                     nat.NAT

	wg      *sync.WaitGroup
	closeCh chan bool

	l               sync.RWMutex
	lastServerAddr  net.Addr
	lastFingerprint []byte
	remoteAddr      net.Addr
	peers           map[string]net.Addr
	closed          bool
}

var errNoHelloPeer = errors.New("no messages from peers or server received")

// NewPeer intializes a *Peer instance and communicates with the server at the
// given address to discover other peers. The only supported value for network
// right now is "udp".
//
// If PeerOpts is nil all default values will be used.
//
// Canceling the context after this function has returned successfully has no
// effect.
func NewPeer(ctx context.Context, network, serverAddr string, opts *PeerOpts) (*Peer, error) {
	if network != "udp" {
		panic("only network 'udp' is supported by NewPeer")
	} else if opts == nil {
		opts = new(PeerOpts)
	}

	var err error
	peer := &Peer{
		po:            (*opts).withDefaults(),
		network:       network,
		serverAddrStr: serverAddr,
		wg:            new(sync.WaitGroup),
		closeCh:       make(chan bool),
	}

	peer.PacketConn, err = net.ListenPacket(peer.network, peer.po.ListenAddr)
	if err != nil {
		return nil, err
	}

	innerCtx := ctx
	if peer.po.InitTimeoutUntilGateway > 0 {
		var cancel func()
		innerCtx, cancel = context.WithTimeout(ctx, peer.po.InitTimeoutUntilGateway)
		defer cancel()
	}

	err = peer.meetPeer(innerCtx)
	if peer.po.InitTimeoutUntilGateway > 0 && err == errNoHelloPeer {
		// TODO gateway stuff
		if peer.gw, err = nat.DiscoverGateway(ctx); err != nil {
			peer.Close()
			return nil, err
		} else if err := peer.natForward(); err != nil {
			peer.Close()
			return nil, err
		}

		err = peer.meetPeer(ctx)
	}
	if err != nil {
		peer.Close()
		return nil, err
	}

	if peer.po.ReadyToMingleInterval > 0 {
		// If readyToMingle errors at this point it's because it couldn't
		// resolve the server or sending failed. The server is known to be
		// resolvable already, and we know we can send on our connection too. So
		// assume the problem is temporary and continue on.
		peer.readyToMingle()
		peer.wg.Add(1)
		go peer.spinReadyToMingle()
	}

	if peer.gw != nil {
		peer.wg.Add(1)
		go peer.spinNATForward()
	}

	return peer, nil
}

func (p *Peer) meetPeer(ctx context.Context) error {
	if err := p.resetPeers(); err != nil {
		return err
	} else if err = p.waitForPeer(ctx); err == context.DeadlineExceeded {
		return errNoHelloPeer
	}
	return nil
}

func (p *Peer) readyToMingle() error {
	p.l.Lock()
	serverAddr, err := p.serverAddr()
	if err != nil {
		p.l.Unlock()
		return err
	}
	p.l.Unlock()

	return multiSend(serverAddr, p.PacketConn, p.po.PacketBlastCount, Message{
		Fingerprint: p.lastFingerprint,
		Type:        ReadyToMingle,
	})
}

func (p *Peer) spinReadyToMingle() {
	defer p.wg.Done()
	t := time.NewTicker(p.po.ReadyToMingleInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			p.readyToMingle()
		case <-p.closeCh:
			return
		}
	}
}

func (p *Peer) localPort() int {
	// we panic in here because there's really no reason these shouldn't work
	addrStr := p.PacketConn.LocalAddr().String()
	_, portStr, err := net.SplitHostPort(addrStr)
	if err != nil {
		panic(err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(err)
	}
	return port
}

func (p *Peer) natForward() error {
	_, err := p.gw.AddPortMapping(
		p.PacketConn.LocalAddr().Network(),
		p.localPort(),
		"port forwarding for bonfire peer",
		p.po.GatewayPortMapTimeout,
	)
	return err
}

func (p *Peer) spinNATForward() {
	defer p.wg.Done()
	t := time.NewTicker(p.po.GatewayPortMapTimeout / 4)
	defer t.Stop()
	proto := p.PacketConn.LocalAddr().Network()
	for {
		select {
		case <-t.C:
			p.natForward()
		case <-p.closeCh:
			p.gw.DeletePortMapping(proto, p.localPort())
			return
		}
	}
}

// PeerAddrs returns the addresses of all currently known peers of this Peer.
func (p *Peer) PeerAddrs() []net.Addr {
	p.l.RLock()
	defer p.l.RUnlock()
	addrs := make([]net.Addr, 0, len(p.peers))
	for _, addr := range p.peers {
		addrs = append(addrs, addr)
	}
	return addrs
}

// RemoteAddr returns the remote address for this Peer, as gathered by
// communicating with other peers and the server.
func (p *Peer) RemoteAddr() net.Addr {
	p.l.RLock()
	defer p.l.RUnlock()
	return p.remoteAddr
}

// we re-resolve this every time in case it is a hostname.
func (p *Peer) serverAddr() (net.Addr, error) {
	addr, err := net.ResolveUDPAddr(p.network, p.serverAddrStr)
	if err != nil {
		return nil, err
	}
	p.lastServerAddr = addr
	return addr, nil
}

func (p *Peer) fingerprint() ([]byte, error) {
	var err error
	var fingerprint []byte
	if p.po.FingerprintFunc == nil {
		fingerprint = make([]byte, FingerprintSize)
		_, err = rand.Read(fingerprint)
	} else {
		fingerprint, err = p.po.FingerprintFunc()
		if len(fingerprint) != FingerprintSize {
			return nil, errors.New("generated fingerprint is not correct size")
		}
	}
	if err != nil {
		return nil, err
	}
	p.lastFingerprint = fingerprint
	return fingerprint, nil
}

func (p *Peer) resetPeers() error {
	p.peers = map[string]net.Addr{}

	fingerprint, err := p.fingerprint()
	if err != nil {
		return err
	}

	serverAddr, err := p.serverAddr()
	if err != nil {
		return err
	}

	return multiSend(serverAddr, p, p.po.PacketBlastCount, Message{
		Fingerprint: fingerprint,
		Type:        HelloServer,
	})
}

// ResetPeers clears the internal list of known peers and sends a message to the
// server to retrieve some more. Once this is called ReadFrom will need to be
// called repeatedly, even if it's not otherwise being used, in order to collect
// the hello messages from peers.
func (p *Peer) ResetPeers() error {
	p.l.Lock()
	defer p.l.Unlock()
	return p.resetPeers()
}

// returns errNoHelloPeer if it didn't receive any messages at all.
// p.peerAddrs may be empty if there are no other peers, but in that case the
// server should at least send something.
func (p *Peer) waitForPeer(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		b := make([]byte, MaxMessageSize)
		p.PacketConn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, addr, err := p.PacketConn.ReadFrom(b)
		if err != nil {
			if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
				continue
			}
			return err
		}

		var msg Message
		if err := msg.UnmarshalBinary(b[:n]); err != nil {
			continue
		} else if msg.Type != HelloPeer {
			continue
		}

		return p.processMessage(addr, msg)
	}
}

// ReadFrom implements the method for the net.PacketConn interface. It will
// process all incoming packets, implicitly handling any bonfire packets and
// passing on others to the caller.
//
// The length of the passed in b must be at least MaxMessageSize.
func (p *Peer) ReadFrom(b []byte) (int, net.Addr, error) {
	if len(b) < MaxMessageSize {
		return 0, nil, errors.New("length of []byte passed into ReadFrom must be at least bonfire.MaxMessageSize")
	}

	for {
		n, addr, err := p.PacketConn.ReadFrom(b)
		if err != nil || n > MaxMessageSize || n < MinMessageSize || b[0] != 0 {
			return n, addr, err
		}

		p.l.RLock()
		lastFingerprint := p.lastFingerprint
		p.l.RUnlock()
		if !bytes.Equal(b[1:1+FingerprintSize], lastFingerprint) {
			return n, addr, nil
		}

		var msg Message
		if err := msg.UnmarshalBinary(b[:n]); err != nil {
			return n, addr, nil
		}

		// from this point on assume it's a bonfire message, any errors
		// encountered will be ignored
		p.l.Lock()
		p.processMessage(addr, msg)
		p.l.Unlock()
	}
}

func (p *Peer) processMessage(addr net.Addr, msg Message) error {
	switch msg.Type {
	case Meet:
		return multiSend(msg.MeetBody.Addr, p, p.po.PacketBlastCount, Message{
			Fingerprint: msg.MeetBody.Fingerprint,
			Type:        HelloPeer,
			HelloPeerBody: HelloPeerBody{
				Addr: msg.MeetBody.Addr,
			},
		})
	case HelloPeer:
		if p.remoteAddr == nil {
			p.remoteAddr = msg.HelloPeerBody.Addr
		}
		addrString := addr.String()
		if addrString == p.lastServerAddr.String() {
			break
		}
		if len(p.peers) >= p.po.MaxPeers {
			for peerAddrStr := range p.peers {
				delete(p.peers, peerAddrStr)
				break
			}
		}
		p.peers[addrString] = addr
	}
	return nil
}

// Close closes the underlying PacketConn and cleans up all other resources used
// by Peer.
func (p *Peer) Close() error {
	p.l.Lock()
	defer p.l.Unlock()

	if p.closed {
		return errors.New("bonfire.Peer already closed")

	} else if err := p.PacketConn.Close(); err != nil {
		return err
	}
	close(p.closeCh)
	p.wg.Wait()
	p.closed = true
	return nil
}
