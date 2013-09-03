package iproto

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"sort"
)

var _ = log.Print

const (
	mrInFly  = 0
	mrFailed = mrCancel | mrExpire
)
const (
	mrChan = (1 << iota)
	mrWait
	mrCancel
	mrExpire
	mrFailPerformed
)

const (
	mrBufSize = 16
)

type MultiResponse []*Response
var _ sort.Interface = MultiResponse(nil)

func (mr MultiResponse) Less(i, j int) bool {
	return mr[i].Id < mr[j].Id
}

func (mr MultiResponse) Swap(i, j int) {
	mr[i], mr[j] = mr[j], mr[i]
}

func (mr MultiResponse) Len() int {
	return len(mr)
}

func (mr MultiResponse) Sort() MultiResponse {
	sort.Sort(mr)
	return mr
}

type MultiRequest struct {
	cx        *Context
	m         sync.Mutex
	c, r      uint32
	requests  []*Request
	responses MultiResponse
	ch        chan *Response
	w         sync.Cond
	timer     Timer
	timerSet  bool
	kind      uint32
	bodyn     uint32
}

func (w *MultiRequest) TimeoutFrom(d Service) {
	w.SetTimeout(d.DefaultTimeout())
}

func (w *MultiRequest) SetTimeout(timeout time.Duration) {
	if timeout > 0 && !w.timerSet {
		w.timerSet = true
		w.timer.E = w
		w.timer.After(timeout)
	}
}

func (w *MultiRequest) Request(msg RequestType, body IWriter) *Request {
	if w.cx == nil {
		w.cx = &Context{}
	}
	req := w.cx.request(uint32(len(w.requests)), msg, body)
	req.Responder = w
	req.timerSet = w.timerSet
	if len(w.requests) == cap(w.requests) {
		w.m.Lock()
		if cap(w.requests) == 0 {
			w.requests = make([]*Request, 1, cxReqBuf)
			w.requests[0] = req
		} else {
			w.requests = append(w.requests, req)
		}
		w.m.Unlock()
	} else {
		w.requests = append(w.requests, req)
	}
	atomic.StoreUint32(&w.r, uint32(len(w.requests)))
	if atomic.LoadUint32(&w.kind)&mrFailed != 0 {
		w.performFail(req)
	}
	return req
}

func (w *MultiRequest) SendMsgBody(serv Service, msg RequestType, body interface{}) *Request {
	wr, _ := Wrap2IWriter(body)
	req := w.Request(msg, wr)
	serv.Send(req)
	return req
}

func (w *MultiRequest) Send(serv Service, r RequestData) *Request {
	req := w.Request(r.IMsg(), r)
	serv.Send(req)
	return req
}

func (w *MultiRequest) Each() <-chan *Response {
	if w.kind&mrFailed != 0 && w.c != w.r {
		w.performFailAll()
	}
	w.m.Lock()
	w.setKind(mrChan)
	w.ch = make(chan *Response, w.r)

	for _, resp := range w.responses {
		w.requests[resp.Id] = nil
		w.ch <- resp
	}
	w.responses = nil
	if w.c == w.r {
		close(w.ch)
	}
	w.m.Unlock()
	return w.ch
}

func (w *MultiRequest) Results() MultiResponse {
	if w.kind&mrFailed != 0 && w.c != w.r {
		w.performFailAll()
	}
	w.m.Lock()
	w.setKind(mrWait)
	w.w.L = &w.m
	if cap(w.responses) < len(w.requests) {
		tmp := make([]*Response, len(w.responses), len(w.requests))
		copy(tmp, w.responses)
		w.responses = tmp
	}
	if atomic.LoadUint32(&w.c) < w.r {
		w.w.Wait()
	}
	res := w.responses
	w.responses = nil
	w.cx.RemoveCanceler(w)
	w.m.Unlock()
	return res
}

func (w *MultiRequest) Respond(r *Response) {
	if w.ch == nil {
		w.m.Lock()
		if w.ch == nil {
			w.responses = append(w.responses, r)
			w.requests[r.Id] = nil
			if w.c++; w.c == w.r && w.kind&mrWait != 0 {
				w.timer.Stop()
				w.w.Signal()
			}
			w.m.Unlock()
			return
		}
		w.m.Unlock()
	}
	w.ch <- r
	if v := atomic.AddUint32(&w.c, 1); v == w.r {
		w.timer.Stop()
		close(w.ch)
		w.cx.RemoveCanceler(w)
	}
}

func (w *MultiRequest) Cancel() {
	if w.c == w.r {
		return
	}
	w.setKind(mrCancel)
	w.performFailAll()
}

func (w *MultiRequest) Expire() {
	if w.c == w.r {
		return
	}
	w.setKind(mrExpire)
	w.performFailAll()
}

func (w *MultiRequest) performFailAll() {
	w.m.Lock()
	r := int(atomic.LoadUint32(&w.r))
	allReqs := w.requests
	w.m.Unlock()

	for i := 0; i < r; i++ {
		if allReqs[i] != nil {
			w.performFail(allReqs[i])
		}
	}
}

func (w *MultiRequest) performFail(r *Request) {
	switch {
	case w.kind&mrCancel != 0:
		r.Cancel()
	case w.kind&mrExpire != 0:
		r.Expire()
	}
}

func (w *MultiRequest) setKind(k uint32) {
	kind := w.kind
	for !atomic.CompareAndSwapUint32(&w.kind, kind, kind|k) {
		kind = w.kind
	}
}
