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

// MessageType enumerates the type of a bonfire message being sent/received.
type MessageType byte

// Possible bonfire message types
const (
	Hello MessageType = iota
	Meet
	ReadyToMingle

	invalid
)

func (mt MessageType) String() string {
	switch mt {
	case Hello:
		return "Hello"
	case Meet:
		return "Meet"
	case ReadyToMingle:
		return "ReadyToMingle"
	default:
		panic(fmt.Sprintf("unknown MessageType: %q", byte(mt)))
	}
}

// MeetBody describes further fields which are used for Meet messages.
type MeetBody struct {
	Addr string
}

func (mb MeetBody) splitHostPort() ([]byte, uint16, error) {
	ipStr, portStr, err := net.SplitHostPort(mb.Addr)
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

// Message describes a bonfire message can be read to or written from a
// connection.
type Message struct {
	Fingerprint []byte // expected to be 64 bytes long
	Type        MessageType

	MeetBody // Only used when Type == Meet
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (m Message) MarshalBinary() ([]byte, error) {
	b := make([]byte, 0, 85)
	b = append(b, 0) // version
	b = append(b, m.Fingerprint[:64]...)
	b = append(b, byte(m.Type))

	if m.Type == Meet {
		b = append(b, 0) // proto:udp
		ip, port, err := m.MeetBody.splitHostPort()
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint16(b[len(b):len(b)+2], port)
		b = b[:len(b)+2]
		b = append(b, ip...)
	}

	return b, nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (m *Message) UnmarshalBinary(b []byte) error {
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

	if m.Type == Meet {
		if proto := read(1); err != nil {
			return err
		} else if proto[0] != 0 {
			return errors.New("malformed message: Meet: invalid proto")
		}
		portB := read(2)
		ip := b
		if err != nil {
			return err
		} else if len(ip) != 4 && len(ip) != 16 {
			return errors.New("malformed message: Meet: invalid ip")
		}

		port := binary.BigEndian.Uint16(portB)
		m.MeetBody.Addr = net.JoinHostPort(net.IP(ip).String(), strconv.Itoa(int(port)))
	}

	return nil
}
