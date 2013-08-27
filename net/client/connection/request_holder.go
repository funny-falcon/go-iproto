package connection

import (
	"github.com/funny-falcon/go-iproto"
	"sync"
	"sync/atomic"
	"log"
)

const (
	rowLogN = 8
	rowN = 1 << rowLogN
	rowMask, rowN1 = rowN-1, rowN-1
)

type RequestRow struct {
	freed uint32
	reqs  [rowN]Request
}

type reqMap map[uint32]*RequestRow
type RequestHolder struct {
	sync.Mutex
	count uint32
	curId uint32
	reqs reqMap
	cur  *RequestRow
	big  uint32
	last *RequestRow
	lastBig uint32
}

func (h *RequestHolder) init() {
	h.cur = &RequestRow{}
	h.last = h.cur
	h.reqs = make(reqMap)
	h.reqs[0] = h.cur
}

func (h *RequestHolder) getNext(conn *Connection) (req *Request) {
	atomic.AddUint32(&h.count, 1)
	for {
		id := atomic.AddUint32(&h.curId, 1)
		big := id>>rowLogN
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

func (h *RequestHolder) get(id uint32) (req *Request, reqs *RequestRow) {
	var ok bool
	big := id>>rowLogN
	if h.lastBig != big {
		h.Lock()
		if h.last, ok = h.reqs[big]; !ok {
			h.Unlock()
			log.Panicf("Map has no RequestRow for %d", id)
		}
		h.lastBig = big
		h.Unlock()
	}
	reqs = h.last
	req = &reqs.reqs[id&rowMask]
	return
}

func (h *RequestHolder) putBack(r *Request, reqs *RequestRow) {
	big := r.fakeId>>rowLogN
	reqs.reqs[r.fakeId&rowMask].fakeId = 0
	border := big == 0 || big == uint32(iproto.PingRequestId>>8)
	reqs.freed++
	if reqs.freed == rowN || (reqs.freed == rowN1 && border) {
		h.Lock()
		delete(h.reqs, big)
		h.Unlock()
	}
	atomic.AddUint32(&h.count, ^uint32(0))
}

func (h *RequestHolder) getAll() (reqs []*Request) {
	h.Lock()
	defer h.Unlock()
	reqs = make([]*Request, h.count)
	i := 0
	for _, row := range h.reqs {
		for _, req := range row.reqs {
			if req.fakeId != 0 && req.Request != nil {
				reqs[i] = &req
				i++
			}
		}
	}
	reqs = reqs[:i]
	return
}
