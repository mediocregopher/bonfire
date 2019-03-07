package main

import (
	"context"
	"net"
	"time"

	"github.com/mediocregopher/bonfire"
	"github.com/mediocregopher/mediocre-go-lib/mcfg"
	"github.com/mediocregopher/mediocre-go-lib/mctx"
	"github.com/mediocregopher/mediocre-go-lib/merr"
	"github.com/mediocregopher/mediocre-go-lib/mlog"
	"github.com/mediocregopher/mediocre-go-lib/mrun"
	"github.com/vmihailenco/msgpack"
)

type msgEvent struct {
	Msg
	PeerAddr string
	TS       time.Time
}

type peer struct {
	ctx context.Context
	*bonfire.Peer

	msgCh  chan msgEvent
	stopCh chan struct{}
}

func withPeer(ctx context.Context) (context.Context, *peer) {
	peer := peer{
		ctx:    mctx.NewChild(ctx, "peer"),
		msgCh:  make(chan msgEvent, 128),
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

		peer.ctx = mctx.Annotate(peer.ctx,
			"remote-addr", peer.Peer.RemoteAddr().String())
		mlog.Info("peering completed", peer.ctx)

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

func (peer *peer) spin() error {
	b := make([]byte, 512)
	for {
		select {
		case <-peer.stopCh:
			return nil
		default:
		}

		peer.Peer.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, peerAddr, err := peer.ReadFrom(b)
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			continue
		} else if err != nil {
			return merr.Wrap(err, peer.ctx)
		}

		now := time.Now()

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

		peer.msgCh <- msgEvent{
			Msg:      msg,
			PeerAddr: peerAddr.String(),
			TS:       now,
		}
	}
}

// Send sends the given Msg to the given addrs
func (peer *peer) Send(msg Msg, dstAddrs ...string) error {
	b, err := msgpack.Marshal(msg)
	if err != nil {
		return merr.Wrap(err, peer.ctx)
	}

	for _, addr := range dstAddrs {
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return merr.Wrap(err, mctx.Annotate(peer.ctx, "addr", addr))
		} else if _, err := peer.WriteTo(b, udpAddr); err != nil {
			return merr.Wrap(err, mctx.Annotate(peer.ctx, "addr", addr))
		}
	}
	return nil
}
