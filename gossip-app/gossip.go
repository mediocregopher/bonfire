// Package gossip contains functionality which is shared across the different
// components of the gossip testing framework
package gossip

import (
	"encoding/json"
	"io"

	"github.com/mediocregopher/mediocre-go-lib/merr"
)

// CoordMsgType describes the type of a particular coordination message.
type CoordMsgType string

// Enumeration of the different coordination message types.
const (
	CoordMsgTypeHello    CoordMsgType = "hello"
	CoordMsgTypeNeed     CoordMsgType = "need"
	CoordMsgTypeHave     CoordMsgType = "have"
	CoordMsgTypeDontHave CoordMsgType = "dontHave"
)

// CoordMsg describes any of the CoordMsg types available in this package.
type CoordMsg interface {
	Type() CoordMsgType
}

// CoordMsgHello is sent from the actor to the coordinator to start off the
// communication.
type CoordMsgHello struct {
	Addr string // the peer addr of the actor
}

// Type implements the method for the CoordMsg interface.
func (*CoordMsgHello) Type() CoordMsgType {
	return CoordMsgTypeHello
}

// CoordMsgNeed is used by the coordinator to tell an actor that it needs a
// resource.
type CoordMsgNeed struct {
	Resource string
}

// Type implements the method for the CoordMsg interface.
func (*CoordMsgNeed) Type() CoordMsgType {
	return CoordMsgTypeNeed
}

// CoordMsgHave is used by the coordinator to tell an actor that it has a
// resource.
type CoordMsgHave struct {
	Resource string
}

// Type implements the method for the CoordMsg interface.
func (*CoordMsgHave) Type() CoordMsgType {
	return CoordMsgTypeHave
}

// CoordMsgDontHave is used by the coordinator to tell an actor that it no
// longer has a resource.
type CoordMsgDontHave struct {
	Resource string
}

// Type implements the method for the CoordMsg interface.
func (*CoordMsgDontHave) Type() CoordMsgType {
	return CoordMsgTypeDontHave
}

// CoordConn wraps an io.ReadWriteCloser to enable encoding/decoding CoordMsgs.
type CoordConn struct {
	rwc io.ReadWriteCloser
	enc *json.Encoder
	dec *json.Decoder
}

// NewCoordConn returns a new CoordConn which wraps the ReadWriteCloser. The
// ReadWriteCloser should not be used once passed in.
func NewCoordConn(rwc io.ReadWriteCloser) *CoordConn {
	return &CoordConn{
		rwc: rwc,
		enc: json.NewEncoder(rwc),
		dec: json.NewDecoder(rwc),
	}
}

type coordMsgWrap struct {
	Type  CoordMsgType
	Inner interface{}
}

// Encode encodes any of the CoordMsg types onto the underlying io.Writer.
func (cc *CoordConn) Encode(msg CoordMsg) error {
	return cc.enc.Encode(coordMsgWrap{
		Type:  msg.Type(),
		Inner: msg,
	})
}

// Decode decodes a single coordination message off the CoordConn. The returned
// type will be one of the CoordMsg structs, and will be a pointer.
func (cc *CoordConn) Decode() (CoordMsg, error) {
	var raw json.RawMessage
	if err := cc.dec.Decode(&raw); err != nil {
		return nil, merr.Wrap(err)
	}

	var wrap coordMsgWrap
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, merr.Wrap(err)
	}

	var err error
	switch wrap.Type {
	case CoordMsgTypeHello:
		wrap.Inner = &CoordMsgHello{}
	case CoordMsgTypeNeed:
		wrap.Inner = &CoordMsgNeed{}
	case CoordMsgTypeHave:
		wrap.Inner = &CoordMsgHave{}
	case CoordMsgTypeDontHave:
		wrap.Inner = &CoordMsgDontHave{}
	default:
		return nil, merr.New("unknown msg type")
	}

	err = json.Unmarshal(raw, &wrap)
	return wrap.Inner.(CoordMsg), merr.Wrap(err)
}

// Close calls Close on the underlying io.Closer.
func (cc *CoordConn) Close() error {
	return cc.rwc.Close()
}
