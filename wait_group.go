package iproto

import (
	"sync"
	"github.com/funny-falcon/go-iproto/util"
	"log"
)

const (
	wgInFly = iota
	wgChan
	wgWait
)

type WaitGroup struct {
	m sync.Mutex
	c util.Atomic
	bufn int32
	buf *[16]Request
	requests []*Request
	responses []Response
	cancel chan bool
	ch chan Response
	w sync.Mutex
	kind int32
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

func (w *WaitGroup) Each() <-chan Response {
	w.m.Lock()
	w.kind = wgChan
	w.ch = make(chan Response, len(w.requests))
	select {
	case <-w.cancel:
		w.m.Unlock()
		close(w.ch)
		return w.ch
	default:
	}
	for _, resp := range w.responses {
		w.requests[resp.Id] = nil
		w.ch <- resp
	}
	w.responses = nil
	if int(w.c) == len(w.requests) {
		close(w.ch)
	}
	w.m.Unlock()
	return w.ch
}

func (w *WaitGroup) Results() []Response {
	w.m.Lock()
	w.kind = wgWait
	l := len(w.requests)
	if cap(w.responses) < l {
		tmp := make([]Response, len(w.responses), l)
		copy(tmp, w.responses)
		w.responses = tmp
	}
	w.w.Lock()
	w.m.Unlock()
	if int(w.c.Get()) < len(w.requests) {
		w.w.Lock()
	}
	res := w.responses
	w.responses = nil
	return res
}

func (w *WaitGroup) Respond(r Response) {
	w.requests[r.Id] = nil
	if w.ch == nil {
		w.m.Lock()
		if w.ch == nil {
			w.responses = append(w.responses, r)
			w.incLocked()
			w.m.Unlock()
			return
		}
		w.m.Unlock()
	}
	w.ch <- r
	w.inc()
}

func (w *WaitGroup) inc() {
	if v := w.c.Incr(); int(v) == len(w.requests) {
		w.m.Lock()
		switch w.kind {
		case wgChan:
			close(w.ch)
		case wgWait:
			w.w.Unlock()
		}
		w.m.Unlock()
	}
}

func (w *WaitGroup) incLocked() {
	if w.c++; int(w.c) == len(w.requests) {
		switch w.kind {
		case wgChan:
			close(w.ch)
		case wgWait:
			w.w.Unlock()
		}
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
