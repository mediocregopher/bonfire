package bonfire

import (
	"context"
	"net"
)

// Server implements a bonfire server which can listen and handle clients on a
// single network address.
type Server struct {
	// Errors encountered when interacting with peers will be written
	// here. If nil or if the channel blocks errors will be dropped.
	ErrCh chan<- error

	// When sending a packet to a peer, determines how many times the packet is
	// sent (in case any are dropped). Default is 3.
	PacketBlastCount int

	conn net.PacketConn // created and set during Listen

	mingleCh chan net.Addr
}

// NewServer instantiates and returns a usable Server instance. Public fields on
// the instance may be modified to change its behavior prior to any methods
// being called, but not after.
func NewServer() *Server {
	return &Server{
		PacketBlastCount: 3,
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

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		b := make([]byte, MaxMessageSize)
		n, srcAddr, err := s.conn.ReadFrom(b)
		if err != nil {
			// TODO implement timeout, so cancelation can kinda work
			return err
		}

		// TODO throttle number of go-routines which can be active
		// TODO defer WaitGroup for cancelation
		go s.handlePacket(b[:n], srcAddr)
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
	for {
		select {
		case s.mingleCh <- addr:
			return
		default:
			<-s.mingleCh
		}
	}
}

func (s *Server) getMinglers(n int) []net.Addr {
	addrs := make([]net.Addr, 0, n)
	for i := 0; i < n; i++ {
		// TODO
	}
	return addrs
}

func (s *Server) multiSend(msg Message, dst net.Addr) error {
	b, err := msg.MarshalBinary()
	if err != nil {
		return err
	}

	// This doesn't use a write timeout, because it ought to happen within a
	// go-routine separate from the message processing, and writing should never
	// really block anyway.
	for i := 0; i < s.PacketBlastCount; i++ {
		if _, err := s.conn.WriteTo(b, dst); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) handlePacket(b []byte, src net.Addr) {
	var msg Message
	if err := msg.UnmarshalBinary(b); err != nil {
		s.err(err)
		return
	}

	// TODO check fingerprint

	switch msg.Type {
	case HelloServer:
		meetMsg := Message{
			Fingerprint: msg.Fingerprint,
			Type:        Meet,
			MeetBody: MeetBody{
				Addr: src.String(),
			},
		}
		numMinglers := 3 // TODO make configurable
		minglers := s.getMinglers(numMinglers)
		for _, minglerAddr := range minglers {
			if err := s.multiSend(meetMsg, minglerAddr); err != nil {
				s.err(err)
			}
		}
		// if the server didn't have as many minglers available as it wanted to,
		// it sends a Hello from itself.
		if len(minglers) < numMinglers {
			helloMsg := Message{
				Fingerprint: msg.Fingerprint,
				Type:        HelloPeer,
				HelloPeerBody: HelloPeerBody{
					Addr: src.String(),
				},
			}
			if err := s.multiSend(helloMsg, src); err != nil {
				s.err(err)
			}
		}

	case ReadyToMingle:
		s.addMingler(src)
	default:
		return
	}
}
