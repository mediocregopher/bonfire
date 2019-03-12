// Package gossip contains functionality which is shared across the different
// components of the gossip testing framework
package gossip

import (
	"io"

	"github.com/mediocregopher/mediocre-go-lib/merr"
	"github.com/vmihailenco/msgpack"
)

// CoordMsgType describes the type of a particular coordination message.
type CoordMsgType int64

// Enumeration of the different coordination message types.
const (
	CoordMsgTypeHello CoordMsgType = iota
	CoordMsgTypeNeed
	CoordMsgTypeHave
	CoordMsgTypeDontHave
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
	enc *msgpack.Encoder
	dec *msgpack.Decoder
}

// NewCoordConn returns a new CoordConn which wraps the ReadWriteCloser. The
// ReadWriteCloser should not be used once passed in.
func NewCoordConn(rwc io.ReadWriteCloser) *CoordConn {
	return &CoordConn{
		rwc: rwc,
		enc: msgpack.NewEncoder(rwc),
		dec: msgpack.NewDecoder(rwc),
	}
}

// Encode encodes any of the CoordMsg types onto the underlying io.Writer.
func (cc *CoordConn) Encode(msg CoordMsg) error {
	if err := cc.enc.EncodeInt64(int64(msg.Type())); err != nil {
		return merr.Wrap(err)
	}
	return merr.Wrap(cc.enc.Encode(msg))
}

// Decode decodes a single coordination message off the CoordConn. The returned
// type will be one of the CoordMsg structs, and will be a pointer.
func (cc *CoordConn) Decode() (CoordMsg, error) {
	typ, err := cc.dec.DecodeInt64()
	if err != nil {
		return nil, merr.Wrap(err)
	}

	var res interface{}
	switch CoordMsgType(typ) {
	case CoordMsgTypeHello:
		res = &CoordMsgHello{}
	case CoordMsgTypeNeed:
		res = &CoordMsgNeed{}
	case CoordMsgTypeHave:
		res = &CoordMsgHave{}
	case CoordMsgTypeDontHave:
		res = &CoordMsgDontHave{}
	default:
		return nil, merr.New("unknown msg type")
	}

	err = cc.dec.Decode(res)
	return res.(CoordMsg), merr.Wrap(err)
}

// Close calls Close on the underlying io.Closer.
func (cc *CoordConn) Close() error {
	return cc.rwc.Close()
}
