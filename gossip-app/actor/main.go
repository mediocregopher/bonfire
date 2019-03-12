package main

/*

eg-app is an example app where peers declare either their possession or their
need for arbitrary resources, with resources being identified by some unique
(and mostly arbitrary) string.

*/

import (
	"context"
	"time"

	"github.com/mediocregopher/bonfire/gossip-app"
	"github.com/mediocregopher/mediocre-go-lib/m"
	"github.com/mediocregopher/mediocre-go-lib/mctx"
	"github.com/mediocregopher/mediocre-go-lib/merr"
	"github.com/mediocregopher/mediocre-go-lib/mlog"
	"github.com/mediocregopher/mediocre-go-lib/mrun"
)

// MsgType denotes what kind of information is being conveyed in a Msg.
type MsgType int

// The possible values of MsgType.
const (
	MsgTypeHave MsgType = iota
	MsgTypeDontHave
	MsgTypeNeeds
)

// Msg describes the structure of a message which is gossiped around the
// network.
type Msg struct {
	MsgType MsgType `db:"state"`

	// These two values form a uniqueness key. In other words, a peer can only
	// have one state ("has", "needs", etc...) per resource.
	Addr     string // host:port
	Resource string

	// Used when a peer is sending messages to denote message order to other
	// peers.
	Nonce uint64
}

type app struct {
	peer *peer
	db   *db

	coordConn  *coordConn
	coordMsgCh chan gossip.CoordMsg
	resources  map[string]bool
}

const peerActiveTimeout = 5 * time.Minute

func (app *app) allPeers() (map[string]struct{}, error) {
	m := make(map[string]struct{})
	for _, addr := range app.peer.PeerAddrs() {
		m[addr.String()] = struct{}{}
	}

	dbPeerAddrs, err := app.db.peers(time.Now().Add(-peerActiveTimeout))
	if err != nil {
		return m, err
	}
	for _, addr := range dbPeerAddrs {
		m[addr] = struct{}{}
	}
	return m, nil
}

func (app *app) spray(msg Msg) error {
	addrsM, err := app.allPeers()
	if err != nil {
		return err
	}

	addrs := make([]string, 0, (len(addrsM)/2)+1)
	for addr := range addrsM {
		if len(addrs) == cap(addrs) {
			break
		}
		addrs = append(addrs, addr)
	}

	return app.peer.Send(msg, addrs...)
}

func (app *app) run(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	thisAddr := app.peer.RemoteAddr().String()
	for {
		select {
		case msg := <-app.coordMsgCh:
			ctx := mctx.Annotate(ctx, "msgType", msg.Type())
			mlog.Info("got coord message", ctx)
			switch msgT := msg.(type) {
			// TODO Needs
			case *gossip.CoordMsgHave:
				app.resources[msgT.Resource] = true
			case *gossip.CoordMsgDontHave:
				delete(app.resources, msgT.Resource)
			}

		case msg := <-app.peer.msgCh:
			ctx := mctx.Annotate(ctx,
				"addr", msg.Addr,
				"resource", msg.Resource,
			)
			mlog.Info("got peer message", ctx)
			var err error
			switch msg.MsgType {
			case MsgTypeHave, MsgTypeDontHave:
				err = app.db.recordHave(msg)
			case MsgTypeNeeds:
				var peerAddrs []string
				since := time.Now().Add(-peerActiveTimeout)
				if peerAddrs, err = app.db.peersWith(msg.Resource, since); err != nil {
					break
				}

				// if the msg was sent on behalf of a different peer, send the
				// responses to both the sender and the original requester, so
				// the sender can have it stored for themselves if they or
				// someone else needs to know
				dstAddrs := make([]string, 0, 2)
				dstAddrs = append(dstAddrs, msg.Addr)
				if msg.Addr != msg.PeerAddr {
					dstAddrs = append(dstAddrs, msg.PeerAddr)
				}

				for _, peerAddr := range peerAddrs {
					resMsg := Msg{
						MsgType:  MsgTypeHave,
						Addr:     peerAddr,
						Resource: msg.Resource,
						// TODO this should _probably be the stored nonce for
						// this particular peer/resource
						Nonce: uint64(time.Now().UnixNano()),
					}
					if err = app.peer.Send(resMsg, dstAddrs...); err != nil {
						break
					}
				}
			}
			if err != nil {
				mlog.Warn("error processing msg", ctx, merr.Context(err))
			}

		case <-ticker.C:
			for resource := range app.resources {
				msg := Msg{
					MsgType:  MsgTypeHave,
					Addr:     thisAddr,
					Resource: resource,
					Nonce:    uint64(time.Now().UnixNano()),
				}
				mlog.Info("spraying message", mctx.Annotate(ctx,
					"addr", msg.Addr,
					"resource", msg.Resource,
				))
				if err := app.spray(msg); err != nil {
					mlog.Warn("error spraying msg", ctx, merr.Context(err))
				}
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func main() {
	app := app{
		coordMsgCh: make(chan gossip.CoordMsg),
		resources:  map[string]bool{},
	}
	ctx := m.ServiceContext()
	ctx, app.peer = withPeer(ctx)
	ctx, app.db = withDB(ctx)
	ctx, app.coordConn = withCoordConn(ctx)

	// set up app runtime
	threadCtx, threadCancel := context.WithCancel(ctx)
	ctx = mrun.WithStartHook(ctx, func(context.Context) error {
		threadCtx = mrun.WithThreads(threadCtx, 1, func() error {
			thisAddr := app.peer.RemoteAddr().String()
			return app.coordConn.run(threadCtx, thisAddr, app.coordMsgCh)
		})

		threadCtx = mrun.WithThreads(threadCtx, 1, func() error {
			return app.run(threadCtx)
		})
		return nil
	})

	ctx = mrun.WithStopHook(ctx, func(innerCtx context.Context) error {
		threadCancel()
		return mrun.Wait(threadCtx, innerCtx.Done())
	})

	m.StartWaitStop(ctx)
}
