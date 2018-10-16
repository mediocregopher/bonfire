package bonfire

import (
	"container/list"
	"net"
	"sync"
	"time"
)

// zset keeps track of the set of peers which have sent a ReadyToMingle message
// and when they sent it. It tracks both the time-order in which ReadyToMingle
// messages were last received, and order in which peers were last used.
type zset struct {
	sync.Mutex
	timeL  *list.List                  // oldest -> newest
	usageL *list.List                  // most recently used -> never used
	m      map[string][2]*list.Element // addr -> {timeL element, usageL element}
}

type zsetEl struct {
	t           time.Time
	addr        net.Addr
	fingerprint []byte
}

func newZSet() *zset {
	return &zset{
		timeL:  list.New(),
		usageL: list.New(),
		m:      map[string][2]*list.Element{},
	}
}

func (z *zset) add(addr net.Addr, fingerprint []byte) {
	z.Lock()
	defer z.Unlock()

	addrStr := addr.String()
	listEls, ok := z.m[addrStr]
	if ok {
		z.timeL.Remove(listEls[0])
	}

	el := zsetEl{time.Now(), addr, fingerprint}
	listEls[0] = z.timeL.PushBack(el)
	if listEls[1] == nil {
		listEls[1] = z.usageL.PushBack(el)
	} else {
		listEls[1].Value = el
	}
	z.m[addrStr] = listEls
}

func (z *zset) get(n int, expire time.Time) []zsetEl {
	z.Lock()
	defer z.Unlock()

	zEls := make([]zsetEl, 0, n)
	els := make([]*list.Element, 0, n)
	el := z.usageL.Back()
	for {
		if len(zEls) >= n || el == nil {
			break
		}

		zEl := el.Value.(zsetEl)
		if zEl.t.After(expire) {
			zEls = append(zEls, zEl)
			els = append(els, el)
		}

		el = el.Prev()
	}

	for _, el := range els {
		z.usageL.MoveToFront(el)
	}

	return zEls
}

// expire removes all addrs which were added prior to the given time
func (z *zset) expire(t time.Time) {
	z.Lock()
	defer z.Unlock()

	el := z.timeL.Front()
	for {
		if el == nil {
			break
		}

		zEl := el.Value.(zsetEl)
		if zEl.t.After(t) {
			break
		}
		addrStr := zEl.addr.String()

		// once el is removed from timeL its Next won't be usable anymore, so
		// grab that now
		nextEl := el.Next()

		z.timeL.Remove(el)
		usageLEl := z.m[addrStr][1]
		z.usageL.Remove(usageLEl)
		delete(z.m, addrStr)

		el = nextEl
	}
}
