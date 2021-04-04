package connection

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/funny-falcon/go-iproto"
)

type Request struct {
	iproto.Bookmark
	fakeId uint32
}

const (
	rowLogN        = 8
	rowN           = 1 << rowLogN
	rowMask, rowN1 = rowN - 1, rowN - 1
)

type RequestRow struct {
	freed uint32
	reqs  [rowN]Request
}

type reqMap map[uint32]*RequestRow
type RequestHolder struct {
	sync.Mutex
	got     uint64
	put     uint64
	curId   uint32
	reqs    reqMap
	cur     *RequestRow
	big     uint32
	last    *RequestRow
	lastBig uint32
}

func (h *RequestHolder) init() {
	h.cur = &RequestRow{}
	h.last = h.cur
	h.reqs = make(reqMap)
	h.reqs[0] = h.cur
}

func (h *RequestHolder) getNext(conn *Connection) (req *Request) {
	h.got++
	for {
		id := atomic.AddUint32(&h.curId, 1)
		big := id >> rowLogN
		if h.big != big {
			h.big = big
			h.cur = &RequestRow{}
			h.Lock()
			h.reqs[big] = h.cur
			h.Unlock()
		}
		reqs := h.cur
		if id != 0 && id != uint32(iproto.PingRequestId) {
			req = &reqs.reqs[id&rowMask]
			if req.fakeId != 0 {
				continue
			}
			req.fakeId = uint32(id)
			return
		}
	}
}

func (h *RequestHolder) remove(fakeId uint32) (ireq *iproto.Request) {
	var ok bool
	big := fakeId >> rowLogN
	if h.lastBig != big {
		h.Lock()
		if h.last, ok = h.reqs[big]; !ok {
			h.Unlock()
			log.Panicf("Map has no RequestRow for %d", fakeId)
		}
		h.lastBig = big
		h.Unlock()
	}

	reqs := h.last
	reqs.freed++
	border := big == 0 || big == uint32(iproto.PingRequestId>>8)
	if reqs.freed == rowN || (reqs.freed == rowN1 && border) {
		h.Lock()
		delete(h.reqs, big)
		h.Unlock()
	}

	req := &reqs.reqs[fakeId&rowMask]
	req.fakeId = 0
	ireq = req.Request
	h.put++
	return
}

func (h *RequestHolder) getAll() (reqs []*Request) {
	h.Lock()
	defer h.Unlock()
	reqs = make([]*Request, h.got-h.put)
	i := 0
	for _, row := range h.reqs {
		for j := range row.reqs {
			req := &row.reqs[j]
			if req.fakeId != 0 && req.Request != nil {
				reqs[i] = req
				i++
			}
		}
	}
	reqs = reqs[:i]
	return
}

func (h *RequestHolder) count() uint {
	put := atomic.LoadUint64(&h.put)
	got := atomic.LoadUint64(&h.got)
	return uint(got - put)
}
