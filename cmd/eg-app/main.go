package main

/*

eg-app is an example app where peers declare either their possession or their
need for arbitrary resources, with resources being identified by some unique
(and mostly arbitrary) string.

*/

import (
	"context"
	"time"

	//_ "github.com/mattn/go-sqlite3"
	"github.com/mediocregopher/mediocre-go-lib/m"
	"github.com/mediocregopher/mediocre-go-lib/mctx"
	"github.com/mediocregopher/mediocre-go-lib/merr"
	"github.com/mediocregopher/mediocre-go-lib/mlog"
	"github.com/mediocregopher/mediocre-go-lib/mrand"
	"github.com/mediocregopher/mediocre-go-lib/mrun"
)

// MsgType denotes what kind of information is being conveyed in a Msg.
type MsgType int

// The possible values of MsgType.
const (
	MsgTypeHas MsgType = iota
	MsgTypeNeeds
)

// Msg describes the structure of a message which is gossiped around the
// network.
type Msg struct {
	MsgType

	// These two values form a uniqueness key. In other words, a peer can only
	// have one state ("has", "needs", etc...) per resource.
	Addr     string // host:port
	Resource string

	// Used when a peer is sending messages to denote message order to other
	// peers.
	Nonce uint64
}

func main() {
	ctx := m.ServiceContext()
	ctx, peer := withPeer(ctx)

	threadCtx, threadCancel := context.WithCancel(ctx)
	ctx = mrun.WithStartHook(ctx, func(context.Context) error {
		thisAddr := peer.RemoteAddr().String()
		threadCtx = mrun.WithThreads(threadCtx, 1, func() error {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case msg := <-peer.msgCh:
					mlog.Info("got message", mctx.Annotate(threadCtx,
						"addr", msg.Addr,
						"resource", msg.Resource,
					))
				case <-ticker.C:
					msg := Msg{
						Addr:     thisAddr,
						Resource: mrand.Hex(4),
						Nonce:    uint64(time.Now().UnixNano()),
					}
					mlog.Info("spraying message", mctx.Annotate(threadCtx,
						"addr", msg.Addr,
						"resource", msg.Resource,
					))
					if err := peer.Spray(msg); err != nil {
						mlog.Warn("error spraying msg", threadCtx, merr.Context(err))
					}
				case <-threadCtx.Done():
					return nil
				}
			}
		})
		return nil
	})

	ctx = mrun.WithStopHook(ctx, func(innerCtx context.Context) error {
		threadCancel()
		return mrun.Wait(threadCtx, innerCtx.Done())
	})

	//mlog.Info("creating sqlite db", ctx)
	//db, err := sqlx.Connect("sqlite", ":memory:")
	//if err != nil {
	//	mlog.Fatal("failed to initialize sqlite", ctx, merr.Context(err))
	//}

	m.StartWaitStop(ctx)
}
