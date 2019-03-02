package main

/*

eg-app is an example app where peers declare either their possession or their
need for arbitrary resources, with resources being identified by some unique
(and mostly arbitrary) string.

*/

import (
	_ "github.com/mattn/go-sqlite3"
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
	//ctx := m.ServiceContext()

	//mlog.Info("creating sqlite db", ctx)
	//db, err := sqlx.Connect("sqlite", ":memory:")
	//if err != nil {
	//	mlog.Fatal("failed to initialize sqlite", ctx, merr.Context(err))
	//}
}
