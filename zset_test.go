package bonfire

import (
	"container/list"
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

	elsToAddrs := func(els []zsetEl) []string {
		strs := make([]string, len(els))
		for i := range els {
			strs[i] = els[i].addr.String()
		}
		return strs
	}

	type zEl struct {
		addr        string
		fingerprint []byte
	}

	assertEls := func(l *list.List, expZEls ...zEl) massert.Assertion {
		els := lToA(l)
		zEls := make([]zEl, len(els))
		for i := range els {
			zEls[i] = zEl{
				addr:        els[i].addr.String(),
				fingerprint: els[i].fingerprint,
			}
		}

		return massert.AnyOne(
			massert.Equal(zEls, expZEls),
			massert.All(
				massert.Len(expZEls, 0),
				massert.Len(zEls, 0),
			),
		)
	}

	a := "127.0.0.1:0"
	fa := []byte{0xa}
	za := zEl{a, fa}

	b := "127.0.0.2:0"
	fb := []byte{0xb}
	zb := zEl{b, fb}

	c := "127.0.0.3:0"
	fc := []byte{0xc}
	zc := zEl{c, fc}

	d := "127.0.0.4:0"
	fd := []byte{0xd}
	zd := zEl{d, fd}

	e := "127.0.0.5:0"
	fe := []byte{0xe}
	ze := zEl{e, fe}

	t.Run("add", func(t *T) {
		var aa []massert.Assertion
		z := newZSet()
		aa = append(aa, assertEls(z.timeL))
		aa = append(aa, assertEls(z.usageL))
		aa = append(aa, massert.Len(z.m, 0))

		z.add(addrString(a), fa)
		aa = append(aa, assertEls(z.timeL, za))
		aa = append(aa, assertEls(z.usageL, za))
		aa = append(aa, massert.Len(z.m, 1))

		z.add(addrString(b), fb)
		aa = append(aa, assertEls(z.timeL, za, zb))
		aa = append(aa, assertEls(z.usageL, za, zb))
		aa = append(aa, massert.Len(z.m, 2))

		z.add(addrString(a), fc)
		aa = append(aa, assertEls(z.timeL, zb, zEl{a, fc}))
		aa = append(aa, assertEls(z.usageL, zEl{a, fc}, zb))
		aa = append(aa, massert.Len(z.m, 2))

		z.add(addrString(c), fc)
		aa = append(aa, assertEls(z.timeL, zb, zEl{a, fc}, zc))
		aa = append(aa, assertEls(z.usageL, zEl{a, fc}, zb, zc))
		aa = append(aa, massert.Len(z.m, 3))

		massert.Fatal(t, massert.All(aa...))
	})

	t.Run("get", func(t *T) {
		var aa []massert.Assertion
		z := newZSet()

		out := z.get(2, time.Time{})
		aa = append(aa, massert.Len(out, 0))

		z.add(addrString(a), fa)
		z.add(addrString(b), fb)
		z.add(addrString(c), fc)
		z.add(addrString(d), fd)
		z.add(addrString(e), fe)
		aa = append(aa, assertEls(z.timeL, za, zb, zc, zd, ze))
		aa = append(aa, assertEls(z.usageL, za, zb, zc, zd, ze))
		aa = append(aa, massert.Len(z.m, 5))

		addrStrs := elsToAddrs(z.get(2, time.Time{}))
		aa = append(aa, massert.Equal(addrStrs, []string{e, d}))
		aa = append(aa, assertEls(z.timeL, za, zb, zc, zd, ze))
		aa = append(aa, assertEls(z.usageL, zd, ze, za, zb, zc))
		aa = append(aa, massert.Len(z.m, 5))

		aa = append(aa, massert.Len(z.get(2, time.Now()), 0))
		aa = append(aa, assertEls(z.timeL, za, zb, zc, zd, ze))
		aa = append(aa, assertEls(z.usageL, zd, ze, za, zb, zc))
		aa = append(aa, massert.Len(z.m, 5))

		addrStrs = elsToAddrs(z.get(6, time.Time{}))
		aa = append(aa, massert.Equal(addrStrs, []string{c, b, a, e, d}))
		aa = append(aa, assertEls(z.timeL, za, zb, zc, zd, ze))
		aa = append(aa, assertEls(z.usageL, zd, ze, za, zb, zc))
		aa = append(aa, massert.Len(z.m, 5))

		aa = append(aa, massert.Len(z.get(0, time.Time{}), 0))
		aa = append(aa, assertEls(z.timeL, za, zb, zc, zd, ze))
		aa = append(aa, assertEls(z.usageL, zd, ze, za, zb, zc))
		aa = append(aa, massert.Len(z.m, 5))

		massert.Fatal(t, massert.All(aa...))
	})

	t.Run("expire", func(t *T) {
		var aa []massert.Assertion
		z := newZSet()
		z.add(addrString(a), fa)
		time.Sleep(1 * time.Millisecond)
		z.add(addrString(b), fb)
		time.Sleep(1 * time.Millisecond)
		z.add(addrString(c), fc)
		time.Sleep(1 * time.Millisecond)
		z.add(addrString(d), fd)
		time.Sleep(1 * time.Millisecond)
		z.add(addrString(e), fe)
		time.Sleep(1 * time.Millisecond)
		z.get(1, time.Time{}) // mix up the order of usageL a bit

		// get the time b was added, remove a and b
		expire := z.timeL.Front().Next().Value.(zsetEl).t
		z.expire(expire)
		aa = append(aa, assertEls(z.timeL, zc, zd, ze))
		aa = append(aa, assertEls(z.usageL, ze, zc, zd))
		aa = append(aa, massert.Len(z.m, 3))

		z.get(1, time.Time{}) // mixing up the order again
		aa = append(aa, assertEls(z.timeL, zc, zd, ze))
		aa = append(aa, assertEls(z.usageL, zd, ze, zc))
		aa = append(aa, massert.Len(z.m, 3))

		// expire everything
		z.expire(time.Now())
		aa = append(aa, assertEls(z.timeL))
		aa = append(aa, assertEls(z.usageL))
		aa = append(aa, massert.Len(z.m, 0))

		massert.Fatal(t, massert.All(aa...))
	})
}
