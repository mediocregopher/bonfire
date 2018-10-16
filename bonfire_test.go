package bonfire

import (
	"bytes"
	"net"
	"reflect"
	. "testing"

	"github.com/mediocregopher/mediocre-go-lib/mrand"
)

func addrString(str string) net.Addr {
	addr, err := net.ResolveUDPAddr("udp", str)
	if err != nil {
		panic(err)
	}
	return addr
}

func TestMessage(t *T) {
	type testT struct {
		msg Message // Fingerprint will be ignored
		exp []byte  // sans the leading version/fingerprint
	}

	randFingerprint := mrand.Bytes(FingerprintSize)

	tests := []testT{
		{
			Message{Type: HelloServer},
			[]byte{0x0},
		},
		{
			Message{
				Type: HelloPeer,
				HelloPeerBody: HelloPeerBody{
					Addr: addrString("127.0.0.1:6666"),
				},
			},
			[]byte{0x1, 0x0, 0x1a, 0xa, 0x7f, 0x0, 0x0, 0x1},
		},
		{
			Message{
				Type: HelloPeer,
				HelloPeerBody: HelloPeerBody{
					Addr: addrString("[::1]:6666"),
				},
			},
			[]byte{0x1, 0x0, 0x1a, 0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
		},
		{
			Message{
				Type: Meet,
				MeetBody: MeetBody{
					Fingerprint: randFingerprint,
					Addr:        addrString("127.0.0.1:6666"),
				},
			},
			append(
				append([]byte{0x2}, randFingerprint...),
				[]byte{0x0, 0x1a, 0xa, 0x7f, 0x0, 0x0, 0x1}...),
		},
		{
			Message{
				Type: Meet,
				MeetBody: MeetBody{
					Fingerprint: randFingerprint,
					Addr:        addrString("[::1]:6666"),
				},
			},
			append(
				append([]byte{0x2}, randFingerprint...),
				[]byte{0x0, 0x1a, 0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1}...),
		},
		{
			Message{Type: ReadyToMingle},
			[]byte{0x3},
		},
	}

	for _, test := range tests {
		msg := test.msg

		msg.Fingerprint = make([]byte, FingerprintSize)
		mrand.Read(msg.Fingerprint)
		expPrefix := append([]byte{0}, msg.Fingerprint...)

		b, err := msg.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary err:%q test:%#v", err, test)

		} else if !bytes.Equal(b[:len(expPrefix)], expPrefix) {
			t.Fatalf("incorrect marshal output (prefix) b:%#v expPrefix:%#v test:%#v", b, expPrefix, test)

		} else if !bytes.Equal(b[len(expPrefix):], test.exp) {
			t.Fatalf("incorrect marshal output b:%#v test:%#v", b[len(expPrefix):], test)
		}

		var msg2 Message
		if err := msg2.UnmarshalBinary(b); err != nil {
			t.Fatalf("UnmarshalBinary err:%q test:%#v", err, test)

		} else if !reflect.DeepEqual(msg, msg2) {
			t.Fatalf("incorrect unmarshal output msg2:%#v test:%#v", msg2, test)
		}
	}
}
