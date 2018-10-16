package bonfire

import (
	"container/list"
	"net"
	. "testing"
	"time"

	"github.com/mediocregopher/mediocre-go-lib/mtest/massert"
)

func TestZSet(t *T) {
	lToA := func(l *list.List) []zsetEl {
		out := make([]zsetEl, 0, l.Len())
		for el := l.Front(); el != nil; el = el.Next() {
			out = append(out, el.Value.(zsetEl))
		}
		return out
	}

	addrsToStrs := func(addrs []net.Addr) []string {
		strs := make([]string, len(addrs))
		for i := range addrs {
			strs[i] = addrs[i].String()
		}
		return strs
	}

	assertAddrs := func(l *list.List, addrs ...string) massert.Assertion {
		els := lToA(l)
		elAddrs := make([]string, len(els))
		for i := range els {
			elAddrs[i] = els[i].addr.String()
		}

		return massert.AnyOne(
			massert.Equal(elAddrs, addrs),
			massert.All(
				massert.Len(addrs, 0),
				massert.Len(elAddrs, 0),
			),
		)
	}

	a := "127.0.0.1:0"
	b := "127.0.0.2:0"
	c := "127.0.0.3:0"
	d := "127.0.0.4:0"
	e := "127.0.0.5:0"

	t.Run("add", func(t *T) {
		var aa []massert.Assertion
		z := newZSet()
		aa = append(aa, assertAddrs(z.timeL))
		aa = append(aa, assertAddrs(z.usageL))
		aa = append(aa, massert.Len(z.m, 0))

		z.add(addrString(a))
		aa = append(aa, assertAddrs(z.timeL, a))
		aa = append(aa, assertAddrs(z.usageL, a))
		aa = append(aa, massert.Len(z.m, 1))

		z.add(addrString(b))
		aa = append(aa, assertAddrs(z.timeL, a, b))
		aa = append(aa, assertAddrs(z.usageL, a, b))
		aa = append(aa, massert.Len(z.m, 2))

		z.add(addrString(a))
		aa = append(aa, assertAddrs(z.timeL, b, a))
		aa = append(aa, assertAddrs(z.usageL, a, b))
		aa = append(aa, massert.Len(z.m, 2))

		z.add(addrString(c))
		aa = append(aa, assertAddrs(z.timeL, b, a, c))
		aa = append(aa, assertAddrs(z.usageL, a, b, c))
		aa = append(aa, massert.Len(z.m, 3))

		massert.Fatal(t, massert.All(aa...))
	})

	t.Run("get", func(t *T) {
		var aa []massert.Assertion
		z := newZSet()

		out := z.get(2, time.Time{})
		aa = append(aa, massert.Len(out, 0))

		z.add(addrString(a))
		z.add(addrString(b))
		z.add(addrString(c))
		z.add(addrString(d))
		z.add(addrString(e))
		aa = append(aa, assertAddrs(z.timeL, a, b, c, d, e))
		aa = append(aa, assertAddrs(z.usageL, a, b, c, d, e))
		aa = append(aa, massert.Len(z.m, 5))

		addrStrs := addrsToStrs(z.get(2, time.Time{}))
		aa = append(aa, massert.Equal(addrStrs, []string{e, d}))
		aa = append(aa, assertAddrs(z.timeL, a, b, c, d, e))
		aa = append(aa, assertAddrs(z.usageL, d, e, a, b, c))
		aa = append(aa, massert.Len(z.m, 5))

		aa = append(aa, massert.Len(z.get(2, time.Now()), 0))
		aa = append(aa, assertAddrs(z.timeL, a, b, c, d, e))
		aa = append(aa, assertAddrs(z.usageL, d, e, a, b, c))
		aa = append(aa, massert.Len(z.m, 5))

		addrStrs = addrsToStrs(z.get(6, time.Time{}))
		aa = append(aa, massert.Equal(addrStrs, []string{c, b, a, e, d}))
		aa = append(aa, assertAddrs(z.timeL, a, b, c, d, e))
		aa = append(aa, assertAddrs(z.usageL, d, e, a, b, c))
		aa = append(aa, massert.Len(z.m, 5))

		aa = append(aa, massert.Len(z.get(0, time.Time{}), 0))
		aa = append(aa, assertAddrs(z.timeL, a, b, c, d, e))
		aa = append(aa, assertAddrs(z.usageL, d, e, a, b, c))
		aa = append(aa, massert.Len(z.m, 5))

		massert.Fatal(t, massert.All(aa...))
	})

	t.Run("expire", func(t *T) {
		var aa []massert.Assertion
		z := newZSet()
		z.add(addrString(a))
		time.Sleep(1 * time.Millisecond)
		z.add(addrString(b))
		time.Sleep(1 * time.Millisecond)
		z.add(addrString(c))
		time.Sleep(1 * time.Millisecond)
		z.add(addrString(d))
		time.Sleep(1 * time.Millisecond)
		z.add(addrString(e))
		time.Sleep(1 * time.Millisecond)
		z.get(1, time.Time{}) // mix up the order of usageL a bit

		// get the time b was added, remove a and b
		expire := z.timeL.Front().Next().Value.(zsetEl).t
		z.expire(expire)
		aa = append(aa, assertAddrs(z.timeL, c, d, e))
		aa = append(aa, assertAddrs(z.usageL, e, c, d))
		aa = append(aa, massert.Len(z.m, 3))

		z.get(1, time.Time{}) // mixing up the order again
		aa = append(aa, assertAddrs(z.timeL, c, d, e))
		aa = append(aa, assertAddrs(z.usageL, d, e, c))
		aa = append(aa, massert.Len(z.m, 3))

		// expire everything
		z.expire(time.Now())
		aa = append(aa, assertAddrs(z.timeL))
		aa = append(aa, assertAddrs(z.usageL))
		aa = append(aa, massert.Len(z.m, 0))

		massert.Fatal(t, massert.All(aa...))
	})
}
