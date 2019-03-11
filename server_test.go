package bonfire

import (
	"context"
	"net"
	. "testing"
	"time"

	"github.com/mediocregopher/mediocre-go-lib/mrand"
	"github.com/mediocregopher/mediocre-go-lib/mtest/massert"
)

func TestServerPeer(t *T) {
	const serverAddr = "127.0.0.1:4499"
	peerOpts := &PeerOpts{
		// for testing, don't bother with gateway traversal, since this is all
		// over localhost anyway
		InitTimeoutUntilGateway: -1,
		// Force ipv4
		ListenAddr: "127.0.0.1:0",
	}

	udpConn := func(addr net.Addr) *net.UDPConn {
		conn, err := net.DialUDP(addr.Network(), nil, addr.(*net.UDPAddr))
		if err != nil {
			t.Fatal(err)
		}
		return conn
	}

	assertAddr := func(addrA, addrB net.Addr) massert.Assertion {
		return massert.All(
			massert.Equal(addrA.Network(), addrB.Network()),
			massert.Equal(addrA.String(), addrB.String()),
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Log("starting server")
	server := NewServer()
	go func() {
		server.Listen(ctx, "udp", serverAddr)
	}()
	// give server a chance to start listening
	time.Sleep(500 * time.Millisecond)

	////////////////////////////////////////////////////////////////////////////

	t.Log("starting peerA")
	peerA, err := NewPeer(ctx, "udp", serverAddr, peerOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("peerA: %v", peerA.RemoteAddr())

	massert.Require(t,
		assertAddr(peerA.PacketConn.LocalAddr(), peerA.RemoteAddr()),
		massert.Length(peerA.PeerAddrs(), 0),
	)

	// wait a moment to ensure the server processes the ReadyToMingle message
	time.Sleep(500 * time.Millisecond)

	// ensure multiplexing is working
	connA := udpConn(peerA.RemoteAddr())
	bExp := mrand.Bytes(100)
	if _, err := connA.Write(bExp); err != nil {
		t.Fatal(err)
	}

	b := make([]byte, MaxMessageSize)
	n, addr, err := peerA.ReadFrom(b)
	massert.Require(t,
		massert.Nil(err),
		massert.Equal(n, 100),
		assertAddr(connA.LocalAddr(), addr),
		massert.Equal(b[:n], bExp),
	)

	// call ReadFrom on peerA forever
	go func() {
		b := make([]byte, MaxMessageSize)
		for {
			if _, _, err := peerA.ReadFrom(b); err != nil {
				if ctx.Err() != nil {
					return
				} else if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
					continue
				}
				t.Fatal(err)
			}
		}
	}()

	////////////////////////////////////////////////////////////////////////////

	t.Log("starting peerB")
	peerB, err := NewPeer(ctx, "udp", serverAddr, peerOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("peerB: %v", peerB.RemoteAddr())

	// most likely the server's HelloPeer message will be the first one to
	// arrive at peerB, so PeerAddrs will be empty. Read for a moment to capture
	// the HelloPeers from peerA as well
	peerB.SetReadDeadline(time.Now().Add(1 * time.Second))
	if _, _, err := peerB.ReadFrom(b); err == nil {
		t.Fatal("peerB should return an error from ReadFrom")
	} else if nErr, ok := err.(net.Error); !ok || !nErr.Timeout() {
		t.Fatal("peerB should return a timeout error from ReadFrom")
	}

	massert.Require(t,
		assertAddr(peerB.PacketConn.LocalAddr(), peerB.RemoteAddr()),
		massert.Length(peerB.PeerAddrs(), 1),
	)

	massert.Require(t, assertAddr(peerA.RemoteAddr(), peerB.PeerAddrs()[0]))
}
