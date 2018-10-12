// Package bonfire implements a client library for discovering peers in a p2p
// application using a bonfire server.
package bonfire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
)

// MaxMessageSize is the maximum number of bytes a Message could possibly be
// when marshaled.
const MaxMessageSize = 85

// MessageType enumerates the type of a bonfire message being sent/received.
type MessageType byte

// Possible bonfire message types
const (
	HelloServer MessageType = iota
	HelloPeer
	Meet
	ReadyToMingle

	invalid
)

func (mt MessageType) String() string {
	switch mt {
	case HelloServer:
		return "HelloServer"
	case HelloPeer:
		return "HelloPeer"
	case Meet:
		return "Meet"
	case ReadyToMingle:
		return "ReadyToMingle"
	default:
		panic(fmt.Sprintf("unknown MessageType: %q", byte(mt)))
	}
}

func splitHostPort(addr string) ([]byte, uint16, error) {
	ipStr, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, 0, err
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return nil, 0, fmt.Errorf("ip:%q is invalid", ipStr)
	}

	ipB := ip.To4()
	if ipB == nil {
		ipB = ip.To16()
	}

	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, 0, fmt.Errorf("port:%q is invalid", portStr)
	}

	return ipB, uint16(port), nil
}

// MeetBody describes further fields which are used for Meet messages.
type MeetBody struct {
	Addr string
}

// HelloPeerBody describes further fields which are used for HelloPeer messages.
type HelloPeerBody struct {
	Addr string
}

// Message describes a bonfire message can be read to or written from a
// connection.
type Message struct {
	Fingerprint []byte // expected to be 64 bytes long
	Type        MessageType

	HelloPeerBody // Only used when Type == HelloPeer
	MeetBody      // Only used when Type == Meet
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (m Message) MarshalBinary() ([]byte, error) {
	b := make([]byte, 0, MaxMessageSize)
	b = append(b, 0) // version
	b = append(b, m.Fingerprint[:64]...)
	b = append(b, byte(m.Type))

	marshalAddr := func(addr string) error {
		b = append(b, 0) // proto:udp
		ip, port, err := splitHostPort(addr)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint16(b[len(b):len(b)+2], port)
		b = b[:len(b)+2]
		b = append(b, ip...)
		return nil
	}

	var err error
	if m.Type == HelloPeer {
		err = marshalAddr(m.HelloPeerBody.Addr)
	} else if m.Type == Meet {
		err = marshalAddr(m.MeetBody.Addr)
	}

	return b, err
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (m *Message) UnmarshalBinary(b []byte) error {
	if len(b) > MaxMessageSize {
		return errors.New("malformed message: too big")
	}

	var err error
	read := func(n int) []byte {
		if len(b) < n {
			err = errors.New("malformed message: too short")
		}
		if err != nil {
			return nil
		}

		out := b[:n]
		b = b[n:]
		return out
	}

	version := read(1)
	m.Fingerprint = read(64)
	typ := read(1)
	if err != nil {
		return err
	} else if version[0] != 0 {
		return errors.New("malformed message: invalid version")
	}

	m.Type = MessageType(typ[0])
	if m.Type >= invalid {
		return errors.New("malformed message: invalid type")
	}

	unmarshalAddr := func() string {
		if proto := read(1); err != nil {
			return ""
		} else if proto[0] != 0 {
			err = fmt.Errorf("malformed message: %s: invalid proto", m.Type.String())
			return ""
		}
		portB := read(2)
		ip := b
		if err != nil {
			return ""
		} else if len(ip) != 4 && len(ip) != 16 {
			err = fmt.Errorf("malformed message: %s: invalid ip", m.Type.String())
			return ""
		}

		port := binary.BigEndian.Uint16(portB)
		return net.JoinHostPort(net.IP(ip).String(), strconv.Itoa(int(port)))
	}

	if m.Type == HelloPeer {
		m.HelloPeerBody.Addr = unmarshalAddr()

	} else if m.Type == Meet {
		m.MeetBody.Addr = unmarshalAddr()
	}

	return err
}
