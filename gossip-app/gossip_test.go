package gossip

import (
	"bytes"
	"io"
	. "testing"

	"github.com/mediocregopher/mediocre-go-lib/mtest/massert"
)

func TestCoordConnEncodeDecode(t *T) {
	// used to give an io.ReadWriter like bytes.Buffer a no-op Close method.
	type wrap struct {
		io.Closer
		io.ReadWriter
	}
	buf := NewCoordConn(wrap{ReadWriter: new(bytes.Buffer)})

	assertEncDec := func(msg CoordMsg) massert.Assertion {
		decEq := func() massert.Assertion {
			got, err := buf.Decode()
			return massert.All(
				massert.Nil(err),
				massert.Equal(msg, got),
			)
		}

		return massert.All(
			massert.Nil(buf.Encode(msg)),
			decEq(),
		)
	}

	massert.Require(t,
		assertEncDec(&CoordMsgHello{
			Addr: "0.0.0.0:1",
		}),
		assertEncDec(&CoordMsgNeed{
			Resource: "foo",
		}),
		assertEncDec(&CoordMsgHave{
			Resource: "foo",
		}),
		assertEncDec(&CoordMsgDontHave{
			Resource: "foo",
		}),
	)
}
