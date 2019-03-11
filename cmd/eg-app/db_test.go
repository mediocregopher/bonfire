package main

import (
	. "testing"
	"time"

	"github.com/mediocregopher/mediocre-go-lib/mtest"
	"github.com/mediocregopher/mediocre-go-lib/mtest/massert"
)

func TestDB(t *T) {
	ctx := mtest.Context()
	ctx, db := withDB(ctx)

	assertPeers := func(since time.Time, expPeers ...string) massert.Assertion {
		peers, err := db.peers(since)
		return massert.All(
			massert.Nil(err),
			massert.Length(peers, len(expPeers)),
			massert.Subset(peers, expPeers),
		)
	}

	assertPeersWith := func(resource string, since time.Time, expPeers ...string) massert.Assertion {
		peers, err := db.peersWith(resource, since)
		return massert.All(
			massert.Nil(err),
			massert.Length(peers, len(expPeers)),
			massert.Subset(peers, expPeers),
		)
	}

	mtest.Run(ctx, t, func() {
		now := time.Now()
		massert.Require(t, assertPeers(now))

		// test that requesting by time works
		massert.Require(t,
			massert.Nil(db.recordMsg(msgEvent{
				Msg: Msg{
					MsgType:  MsgTypeHas,
					Addr:     "0.0.0.0:1",
					Resource: "foo",
					Nonce:    1,
				},
				TS: now,
			})),
			assertPeers(now, "0.0.0.0:1"),
			assertPeers(now.Add(-1*time.Second), "0.0.0.0:1"),
			assertPeers(now.Add(1*time.Second)),
			assertPeersWith("foo", now, "0.0.0.0:1"),
			assertPeersWith("foo", now.Add(-1*time.Second), "0.0.0.0:1"),
			assertPeersWith("foo", now.Add(1*time.Second)),
		)

		// test that nonces work
		massert.Require(t,
			// nonce is less than previous, so this should get dropped
			massert.Nil(db.recordMsg(msgEvent{
				Msg: Msg{
					MsgType:  MsgTypeNeeds,
					Addr:     "0.0.0.0:1",
					Resource: "foo",
					Nonce:    0,
				},
				TS: now,
			})),
			assertPeersWith("foo", now, "0.0.0.0:1"),

			// nonce is the same as previous, so this should get dropped
			massert.Nil(db.recordMsg(msgEvent{
				Msg: Msg{
					MsgType:  MsgTypeNeeds,
					Addr:     "0.0.0.0:1",
					Resource: "foo",
					Nonce:    1,
				},
				TS: now,
			})),
			assertPeersWith("foo", now, "0.0.0.0:1"),

			// nonce is more than previous, so this should get kept
			massert.Nil(db.recordMsg(msgEvent{
				Msg: Msg{
					MsgType:  MsgTypeNeeds,
					Addr:     "0.0.0.0:1",
					Resource: "foo",
					Nonce:    2,
				},
				TS: now.Add(1 * time.Second),
			})),
			assertPeersWith("foo", now),
		)

		// double check that there's only a single row in the db still
		var count int
		err := db.DB.Get(&count, "SELECT COUNT(*) FROM peer_resources")
		massert.Require(t,
			massert.Nil(err),
			massert.Equal(1, count),
		)
	})
}
