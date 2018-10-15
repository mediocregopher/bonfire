package bonfire

import (
	"crypto/rand"
	"errors"
	"net"
	"time"
)

// PeerOpts are passed to the NewPeer function to affect the Peer's behavior.
type PeerOpts struct {
	// When sending a packet to the server or a peer, determines how many times
	// the packet is sent (in case any are dropped). Default is 3.
	PacketBlastCount int

	// The range of time to wait for other peers to meet the Peer. Once
	// PeerMinWait has elapsed NewPeer will use whatever peers have sent a
	// HelloPeer message. If it hasn't received any it will keep waiting for
	// any, or until PeerMaxWait has elapsed.
	//
	// The default values are 1 second and 5 seconds, respectively.
	PeerMinWait, PeerMaxWait time.Duration

	// The interval on which ReadyToMingle messages are sent. If 0, no
	// ReadyToMingle messages will be sent. Default is 1 * time.Minute.
	ReadyToMingleInterval time.Duration

	// Address to listen on when creating the UDP port. Default is ":0", which
	// means any IP address over a randomly picked port.
	ListenAddr string

	// FingerprintFunc can be used to generate the Message fingerprints used by
	// the Peer. A fingerprint must be exactly FingerprintSize bytes. See
	// Server's FingerprintCheck field for an example of how this might be used.
	FingerprintFunc func() ([]byte, error)
}

func (po PeerOpts) withDefaults() PeerOpts {
	if po.PacketBlastCount == 0 {
		po.PacketBlastCount = 3
	}
	if po.PeerMinWait == 0 {
		po.PeerMinWait = 1 * time.Second
	}
	if po.PeerMaxWait == 0 {
		po.PeerMaxWait = 5 * time.Second
	}
	if po.ReadyToMingleInterval == 0 {
		po.ReadyToMingleInterval = 1 * time.Minute
	}
	if po.ListenAddr == "" {
		po.ListenAddr = ":0"
	}
	return po
}

// Peer implements a bonfire peer which can discover other peers from a bonfire
// server and multiplex bonfire and application packets over a single UDP
// port.
//
// No fields on Peer should be modified, all methods are thread-safe.
type Peer struct {
	PeerOpts
	network, serverAddrStr string
	fingerprint            []byte
	conn                   net.PacketConn
}

// NewPeer intializes a *Peer instance and communicates with the server at the
// given address to discover other peers. The only supported value for network
// right now is "udp".
//
// Until Close is called the Client will hold open the socket it creates to talk
// with the server, and accepts packets from peers over that socket as well (see
// ReadFrom method).
//
// Until Close is called the Client will periodically send ReadyToMingle
// messages (unless the interval is 0 in ClientOpts) to the server so that it
// can help new peers discover itself.
//
// If ClientOpts is nil all default values will be used.
func NewPeer(network, serverAddr string, opts *PeerOpts) (*Peer, error) {
	if network != "udp" {
		panic("only network 'udp' is supported by NewPeer")
	}

	var err error
	peer := &Peer{
		PeerOpts:      (*opts).withDefaults(),
		network:       network,
		serverAddrStr: serverAddr,
	}

	if peer.FingerprintFunc == nil {
		peer.fingerprint = make([]byte, FingerprintSize)
		_, err = rand.Read(peer.fingerprint)
	} else {
		peer.fingerprint, err = peer.FingerprintFunc()
		if len(peer.fingerprint) != FingerprintSize {
			return nil, errors.New("generated fingerprint is not correct size")
		}
	}

	peer.conn, err = net.ListenPacket(peer.network, ":0")
	if err != nil {
		return nil, err
	}

	addr, err := peer.serverAddr()
	if err != nil {
		return nil, err
	}

	err = multiSend(addr, peer.conn, peer.PacketBlastCount, Message{
		Fingerprint: peer.fingerprint,
		Type:        HelloServer,
	})
	if err != nil {
		return nil, err
	}
}

// we re-resolve this every time in case it is a hostname.
func (p *Peer) serverAddr() (net.Addr, error) {
	return net.ResolveUDPAddr(p.network, p.serverAddrStr)
}
