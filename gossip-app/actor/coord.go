package main

import (
	"context"
	"net"
	"time"

	"github.com/mediocregopher/bonfire/gossip-app"
	"github.com/mediocregopher/mediocre-go-lib/mcfg"
	"github.com/mediocregopher/mediocre-go-lib/mctx"
	"github.com/mediocregopher/mediocre-go-lib/merr"
	"github.com/mediocregopher/mediocre-go-lib/mlog"
	"github.com/mediocregopher/mediocre-go-lib/mrun"
)

type coordConn struct {
	ctx  context.Context
	conn net.Conn
	*gossip.CoordConn
}

func withCoordConn(parent context.Context) (context.Context, *coordConn) {
	cc := &coordConn{
		ctx: mctx.NewChild(parent, "coord"),
	}

	var addr *string
	cc.ctx, addr = mcfg.WithString(cc.ctx, "addr", "127.0.0.1:9876", "Address of the coordination server which will tell this actor what to do")

	cc.ctx = mrun.WithStartHook(cc.ctx, func(context.Context) error {
		cc.ctx = mctx.Annotate(cc.ctx, "addr", *addr)
		mlog.Info("dialing coord server", cc.ctx)
		conn, err := net.Dial("tcp", *addr)
		if err != nil {
			return merr.Wrap(err, cc.ctx)
		}
		cc.conn = conn
		cc.CoordConn = gossip.NewCoordConn(conn)
		return nil
	})

	cc.ctx = mrun.WithStopHook(cc.ctx, func(context.Context) error {
		mlog.Info("closing connection to coord server", cc.ctx)
		return cc.CoordConn.Close()
	})

	return mctx.WithChild(parent, cc.ctx), cc
}

// run will block until the given Context is canceled or an error is
// encountered. It never returns nil.
func (cc *coordConn) run(ctx context.Context, peerAddr string, msgCh chan<- gossip.CoordMsg) error {
	err := cc.Encode(&gossip.CoordMsgHello{
		Addr: peerAddr,
	})
	if err != nil {
		return merr.Wrap(err, cc.ctx, ctx)
	}

	doneCh := ctx.Done()
	for {
		select {
		case <-doneCh:
			return merr.Wrap(ctx.Err(), cc.ctx, ctx)
		default:
		}

		cc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		msg, err := cc.Decode()
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			continue
		} else if err != nil {
			return merr.Wrap(err, cc.ctx, ctx)
		}

		msgCh <- msg
	}
}
