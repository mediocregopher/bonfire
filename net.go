package bonfire

import "net"

func multiSend(dst net.Addr, conn net.PacketConn, n int, msg Message) error {
	b, err := msg.MarshalBinary()
	if err != nil {
		return err
	}

	// This doesn't use a write timeout, because it ought to happen within a
	// go-routine separate from the message processing, and writing should never
	// really block anyway.
	for i := 0; i < n; i++ {
		if _, err := conn.WriteTo(b, dst); err != nil {
			return err
		}
	}
	return nil
}
