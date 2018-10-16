package bonfire

import (
	"context"
	"net"
	"sync"
	"time"
)

// Server implements a bonfire server which can listen for and handle peers on a
// single network address.
type Server struct {
	// Errors encountered when interacting with peers will be written
	// here. If nil or if the channel blocks errors will be dropped.
	ErrCh chan<- error

	// When sending a packet to a peer, determines how many times the packet is
	// sent (in case any are dropped). Default is 3.
	PacketBlastCount int

	// When the server receives a HelloServer message from a peer, this number
	// determines how many ready-to-mingle peers will receive a Meet message for
	// it. Default is 3.
	PeersToMeet int

	// The amount of time a peer is considered ready-to-mingle after the server
	// receives a ReadyToMingle packet from it. Default is 2 * time.Minute.
	ReadyToMingleTimeout time.Duration

	// Maximum number of go-routines handling incoming packets at any given
	// moment. Each packet is handled by its own go-routine. Default is 500.
	MaxConcurrent int

	// An optional function which can be used to filter out messages based on
	// their fingerprint. If FingerprintCheck returns false the packet is
	// dropped.
	//
	// One example use-case is the peer and server having a pre-shared key, and
	// the peer using a random set of bytes and an HMAC-SHA1 of those bytes, and
	// setting the fingerprint to the concatenation of those two values. The
	// server can then use FingerprintCheck to ensure that all peers know the
	// pre-shared secret.
	FingerprintCheck func([]byte) bool

	conn       net.PacketConn // created and set during Listen
	mingleZSet *zset
}

// NewServer instantiates and returns a usable Server instance. Public fields on
// the instance may be modified to change its behavior prior to any methods
// being called, but not after.
func NewServer() *Server {
	return &Server{
		PacketBlastCount:     3,
		PeersToMeet:          3,
		ReadyToMingleTimeout: 2 * time.Minute,
		MaxConcurrent:        500,
		mingleZSet:           newZSet(),
	}
}

// Listen blocks while the Server listens for and handles communicating with
// peers on the given address. Currently the only supported network is "udp".
func (s *Server) Listen(ctx context.Context, network, addr string) error {
	if network != "udp" {
		panic("only network 'udp' is supported by Listen")
	}

	var err error
	if s.conn, err = net.ListenPacket(network, addr); err != nil {
		return err
	}

	wg := new(sync.WaitGroup)
	defer wg.Wait()

	// set up a routine which will periodically expire out ready-to-mingle peers
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(s.ReadyToMingleTimeout / 2)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.mingleZSet.expire(time.Now().Add(-s.ReadyToMingleTimeout))
			}
		}
	}()

	// set up a throttle. each go-routine will need to read an element from the
	// throttle to be created, and will write the element back when its done.
	throttle := make(chan struct{}, s.MaxConcurrent)
	for i := 0; i < s.MaxConcurrent; i++ {
		throttle <- struct{}{}
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		b := make([]byte, MaxMessageSize)
		s.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, srcAddr, err := s.conn.ReadFrom(b)
		if err != nil {
			if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
				continue
			}
			return err
		}

		<-throttle
		wg.Add(1)
		go func(b []byte, srcAddr net.Addr) {
			defer wg.Done()
			s.handlePacket(b, srcAddr)
			throttle <- struct{}{}
		}(b[:n], srcAddr)
	}
}

func (s *Server) err(err error) {
	if s.ErrCh == nil {
		return
	}
	select {
	case s.ErrCh <- err:
	default:
	}
}

func (s *Server) addMingler(addr net.Addr) {
	s.mingleZSet.add(addr)
}

func (s *Server) getMinglers(n int) []net.Addr {
	return s.mingleZSet.get(n, time.Now().Add(-s.ReadyToMingleTimeout))
}

func (s *Server) handlePacket(b []byte, src net.Addr) {
	var msg Message
	if err := msg.UnmarshalBinary(b); err != nil {
		s.err(err)
		return
	}

	if s.FingerprintCheck != nil && !s.FingerprintCheck(msg.Fingerprint) {
		return
	}

	switch msg.Type {
	case HelloServer:
		minglers := s.getMinglers(s.PeersToMeet)
		for _, minglerAddr := range minglers {
			err := multiSend(minglerAddr, s.conn, s.PacketBlastCount, Message{
				Fingerprint: msg.Fingerprint,
				Type:        Meet,
				MeetBody: MeetBody{
					Addr: src,
				},
			})
			if err != nil {
				s.err(err)
			}
		}
		// if the server didn't have as many minglers available as it wanted to,
		// it sends a Hello from itself.
		if len(minglers) < s.PeersToMeet {
			err := multiSend(src, s.conn, s.PacketBlastCount, Message{
				Fingerprint: msg.Fingerprint,
				Type:        HelloPeer,
				HelloPeerBody: HelloPeerBody{
					Addr: src,
				},
			})
			if err != nil {
				s.err(err)
			}
		}

	case ReadyToMingle:
		s.addMingler(src)
	default:
		return
	}
}
