package iproto

import (
	"sync"
	"github.com/funny-falcon/go-iproto/util"
	"log"
)

type WaitGroup struct {
	m sync.Mutex
	c util.Atomic
	bufn int32
	buf *[16]Request
	requests []*Request
	responses []Response
	ch chan Response
	cancel chan bool
}

func (w *WaitGroup) Init() {
	w.cancel = make(chan bool)
}

func (w *WaitGroup) Request(msg RequestType, body []byte) *Request {
	w.m.Lock()
	if w.buf == nil {
		w.buf = &[16]Request{}
	}
	req := &w.buf[w.bufn]
	if w.bufn++; int(w.bufn) == len(w.buf) {
		w.buf = nil
		w.bufn = 0
	}

	*req = Request{
		Id: uint32(len(w.requests)),
		Msg: msg,
		Body: body,
		Responder: w,
	}
	w.requests = append(w.requests, req)
	req.chainMiddleware(waitGroupMiddleware{w})
	w.m.Unlock()
	return req
}

func (w *WaitGroup) Results() <-chan Response {
	w.m.Lock()
	w.ch = make(chan Response, len(w.requests))
	for _, resp := range w.responses {
		w.requests[resp.Id] = nil
		w.ch <- resp
		w.incLocked()
	}
	w.responses = nil
	w.m.Unlock()
	return w.ch
}

func (w *WaitGroup) Respond(r Response) {
	if w.ch == nil {
		w.m.Lock()
		if w.ch == nil {
			w.responses = append(w.responses, r)
			w.m.Unlock()
			return
		}
		w.m.Unlock()
	}
	w.requests[r.Id] = nil
	w.ch <- r
	w.inc()
}

func (w *WaitGroup) inc() {
	if v := w.c.Incr(); int(v) == len(w.requests) {
		close(w.ch)
	}
}

func (w *WaitGroup) incLocked() {
	if w.c++; int(w.c) == len(w.requests) {
		close(w.ch)
	}
}

func (w *WaitGroup) Cancel() {
	w.m.Lock()
	defer w.m.Unlock()
	select {
	case <-w.cancel:
	default:
		close(w.cancel)
	}
	for i, req := range w.requests {
		if req != nil && req.Cancel() {
			w.requests[i] = nil
			w.incLocked()
		}
	}
}

type waitGroupMiddleware struct {
	*WaitGroup
}
func (w waitGroupMiddleware) Respond(r Response) Response {
	return r
}
func (w waitGroupMiddleware) Cancel() {
}
func (w waitGroupMiddleware) valid() bool {
	return true
}
func (w waitGroupMiddleware) setReq(r *Request, m Middleware) {
	if r.chain != nil {
		log.Panicf("waitGroupMiddleware should be first in chain %+v %+v", m, r.chain)
	}
}
func (w waitGroupMiddleware) unchain() Middleware {
	return nil
}

func (w waitGroupMiddleware) previous() Middleware {
	return nil
}
func (w waitGroupMiddleware) CancelChan() chan bool {
	return w.cancel
}
func (w waitGroupMiddleware) InitChan() {
}
func (w waitGroupMiddleware) CloseChan() {
}
