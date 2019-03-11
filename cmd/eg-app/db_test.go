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

	assertTotalRows := func(expCount int) massert.Assertion {
		// double check that there's only a single row in the db still
		var count int
		err := db.DB.Get(&count, "SELECT COUNT(*) FROM peer_resources")
		return massert.All(
			massert.Nil(err),
			massert.Equal(expCount, count),
		)
	}

	mtest.Run(ctx, t, func() {
		now := time.Now()
		massert.Require(t, assertPeers(now))

		// test that requesting by time works
		massert.Require(t,
			massert.Nil(db.recordHave(msgEvent{
				Msg: Msg{
					MsgType:  MsgTypeHave,
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

		// test that nonces work (recordHave)
		now = now.Add(time.Second)
		massert.Require(t,
			// nonce is less than previous, so this should get dropped
			massert.Nil(db.recordHave(msgEvent{
				Msg: Msg{
					MsgType:  MsgTypeHave,
					Addr:     "0.0.0.0:1",
					Resource: "foo",
					Nonce:    0,
				},
				TS: now,
			})),
			assertPeersWith("foo", now),

			// nonce is the same as previous, so this should get dropped
			massert.Nil(db.recordHave(msgEvent{
				Msg: Msg{
					MsgType:  MsgTypeHave,
					Addr:     "0.0.0.0:1",
					Resource: "foo",
					Nonce:    1,
				},
				TS: now,
			})),
			assertPeersWith("foo", now),

			// nonce is more than previous, so this should get kept
			massert.Nil(db.recordHave(msgEvent{
				Msg: Msg{
					MsgType:  MsgTypeHave,
					Addr:     "0.0.0.0:1",
					Resource: "foo",
					Nonce:    2,
				},
				TS: now,
			})),
			assertPeersWith("foo", now, "0.0.0.0:1"),

			// double check that there's only a single row in the db still
			assertTotalRows(1),
		)

		// test that nonces work (recordDontHave)
		massert.Require(t,
			// nonce is less than previous, so this should get dropped
			massert.Nil(db.recordDontHave(msgEvent{
				Msg: Msg{
					MsgType:  MsgTypeDontHave,
					Addr:     "0.0.0.0:1",
					Resource: "foo",
					Nonce:    1,
				},
				TS: now,
			})),
			assertPeersWith("foo", now, "0.0.0.0:1"),

			// nonce is the same as previous, so this should get dropped
			massert.Nil(db.recordDontHave(msgEvent{
				Msg: Msg{
					MsgType:  MsgTypeDontHave,
					Addr:     "0.0.0.0:1",
					Resource: "foo",
					Nonce:    2,
				},
				TS: now,
			})),
			assertPeersWith("foo", now, "0.0.0.0:1"),

			// nonce is more than previous, so this should get kept
			massert.Nil(db.recordDontHave(msgEvent{
				Msg: Msg{
					MsgType:  MsgTypeDontHave,
					Addr:     "0.0.0.0:1",
					Resource: "foo",
					Nonce:    3,
				},
				TS: now,
			})),
			assertPeersWith("foo", now),

			// double check that there's no rows in the db now
			assertTotalRows(0),
		)
	})
}
