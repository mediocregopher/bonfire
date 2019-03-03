package main

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/mediocregopher/bonfire"
	"github.com/mediocregopher/mediocre-go-lib/mcfg"
	"github.com/mediocregopher/mediocre-go-lib/mctx"
	"github.com/mediocregopher/mediocre-go-lib/merr"
	"github.com/mediocregopher/mediocre-go-lib/mlog"
	"github.com/mediocregopher/mediocre-go-lib/mrun"
	"github.com/vmihailenco/msgpack"
)

type peer struct {
	ctx context.Context
	*bonfire.Peer

	peersL sync.Mutex
	peers  map[string]struct{}

	msgCh  chan Msg
	stopCh chan struct{}
}

func withPeer(ctx context.Context) (context.Context, *peer) {
	peer := peer{
		ctx:    mctx.NewChild(ctx, "peer"),
		peers:  map[string]struct{}{},
		msgCh:  make(chan Msg, 128),
		stopCh: make(chan struct{}),
	}

	var serverAddr *string
	peer.ctx, serverAddr = mcfg.WithString(peer.ctx, "server-addr", "127.0.0.1:7890", "Address of a bonfire server which can be used to find other peers")

	peer.ctx = mrun.WithStartHook(peer.ctx, func(innerCtx context.Context) error {
		peer.ctx = mctx.Annotate(peer.ctx, "server-addr", *serverAddr)
		mlog.Info("peering with bonfire server", peer.ctx, innerCtx)
		var err error
		peer.Peer, err = bonfire.NewPeer(innerCtx, "udp", *serverAddr, nil)
		if err != nil {
			return merr.Wrap(err, peer.ctx, innerCtx)
		}

		peer.setBonfirePeers()

		peer.ctx = mrun.WithThreads(peer.ctx, 1, func() error {
			if err := peer.spin(); err != nil {
				mlog.Fatal("peer loop failed", peer.ctx, merr.Context(err))
			}
			return nil
		})
		return nil
	})

	peer.ctx = mrun.WithStopHook(peer.ctx, func(innerCtx context.Context) error {
		close(peer.stopCh)
		mrun.Wait(peer.ctx, innerCtx.Done())
		close(peer.msgCh)
		return peer.Close()
	})

	return mctx.WithChild(ctx, peer.ctx), &peer
}

// TODO i think it's necessary to make sure that peer doesn't add its own
// remoteAddr to the peers list, though in testing this bug doesn't seem to
// occur?
func (peer *peer) setPeers(addrs ...string) {
	peer.peersL.Lock()
	defer peer.peersL.Unlock()
	for _, addr := range addrs {
		peer.peers[addr] = struct{}{}
	}
}

func (peer *peer) setBonfirePeers() {
	peerAddrs := peer.PeerAddrs()
	addrs := make([]string, len(peerAddrs))
	for i := range peerAddrs {
		addrs[i] = peerAddrs[i].String()
	}
	peer.setPeers(addrs...)
}

func (peer *peer) spin() error {
	setBonfirePeersTicker := time.NewTicker(5 * time.Second)
	defer setBonfirePeersTicker.Stop()

	b := make([]byte, 512)
	for {
		select {
		case <-peer.stopCh:
			return nil
		case <-setBonfirePeersTicker.C:
			peer.setBonfirePeers()
		default:
		}

		peer.Peer.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, peerAddr, err := peer.ReadFrom(b)
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			continue
		} else if err != nil {
			return merr.Wrap(err, peer.ctx)
		}

		var msg Msg
		if err := msgpack.Unmarshal(b[:n], &msg); err != nil {
			mlog.Warn("error unmarshaling msg", peer.ctx, merr.Context(err))
			continue
		} else if ip, _, err := net.SplitHostPort(msg.Addr); err != nil {
			mlog.Warn("msg addr is malformed", peer.ctx, merr.Context(err))
			continue
		} else if net.ParseIP(ip) == nil {
			err := merr.New("invalid ip")
			mlog.Warn("msg addr is malformed", peer.ctx, merr.Context(err))
			continue
		}

		peer.setPeers(peerAddr.String(), msg.Addr)
		peer.msgCh <- msg
	}
}

// Spray sends the given Msg to a random slice of the known set of peers.
func (peer *peer) Spray(msg Msg) error {
	b, err := msgpack.Marshal(msg)
	if err != nil {
		return merr.Wrap(err, peer.ctx)
	}

	addrs := make([]string, 0, 5)
	peer.peersL.Lock()
	for addr := range peer.peers {
		addrs = append(addrs, addr)
		if len(addrs) == cap(addrs) {
			break
		}
	}
	peer.peersL.Unlock()

	for _, addr := range addrs {
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return merr.Wrap(err, mctx.Annotate(peer.ctx, "addr", addr))
		} else if _, err := peer.WriteTo(b, udpAddr); err != nil {
			return merr.Wrap(err, mctx.Annotate(peer.ctx, "addr", addr))
		}
	}
	return nil
}
