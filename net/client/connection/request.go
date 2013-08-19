package connection

import (
	"github.com/funny-falcon/go-iproto"
	"sync"
	"github.com/funny-falcon/go-iproto/util"
	"log"
)

type Request struct {
	iproto.BasicResponder
	fakeId uint32
}

func (r *Request) Respond(res *iproto.Response) {
	res.Id = r.Request.Id
}

const (
	rowLogN = 8
	rowN = 1 << rowLogN
	rowMask, rowN1 = rowN-1, rowN-1
)

type RequestRow struct {
	used util.Atomic
	freed util.Atomic
	reqs  [rowN]Request
}

type reqMap map[util.Atomic]*RequestRow
type RequestHolder struct {
	sync.RWMutex
	count util.Atomic
	curId util.Atomic
	reqs reqMap
}

func (h *RequestHolder) getNext(conn *Connection) *Request {
	h.count.Incr()
	for {
		var reqs *RequestRow
		var ok bool
		id := h.curId.Incr()
		big := id>>rowLogN
		h.RLock()
		reqs, ok = h.reqs[big]
		h.RUnlock()
		if !ok {
			h.Lock()
			if reqs, ok = h.reqs[big]; !ok {
				reqs = &RequestRow{}
				h.reqs[big] = reqs
			}
			h.Unlock()
		}
		if id != 0 && id != util.Atomic(iproto.PingRequestId) {
			req := &reqs.reqs[id&rowMask]
			if req.fakeId != 0 {
				continue
			}
			req.fakeId = uint32(id)
			reqs.used.Incr()
			return req
		}
	}
}

func (h *RequestHolder) get(id uint32) *Request {
	h.RLock()
	defer h.RUnlock()
	var reqs *RequestRow
	var ok bool
	big := util.Atomic(id>>rowLogN)
	if reqs, ok = h.reqs[big]; !ok {
		log.Panicf("Map has no RequestRow for %d", id)
	}
	return &reqs.reqs[id&rowMask]
}

func (h *RequestHolder) putBack(r *Request) {
	if r.fakeId != 0 {
		var reqs *RequestRow
		var ok bool
		big := util.Atomic(r.fakeId>>rowLogN)
		h.RLock()
		if reqs, ok = h.reqs[big]; !ok {
			log.Panicf("Map has no RequestRow for %d", r.fakeId)
		}
		h.RUnlock()
		reqs.reqs[r.fakeId&rowMask].fakeId = 0
		border := big == 0 || big == util.Atomic(iproto.PingRequestId>>8)
		freed := reqs.freed.Incr()
		if freed == rowN || (freed == rowN1 && border) {
			h.Lock()
			delete(h.reqs, big)
			h.Unlock()
		}
		h.count.Decr()
	}
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
	return
}
