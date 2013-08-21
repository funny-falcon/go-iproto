package iproto

import (
	"sync"
	"github.com/funny-falcon/go-iproto/util"
)

type WaitGroup struct {
	m sync.Mutex
	c util.Atomic
	requests []*Request
	responses []Response
	ch chan Response
}

func (w *WaitGroup) Init() {
}

func (w *WaitGroup) Request(msg RequestType, body []byte) *Request {
	w.m.Lock()
	req := &Request{
		Id: uint32(len(w.requests)),
		Msg: msg,
		Body: body,
		Responder: w,
	}
	w.requests = append(w.requests, req)
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
	w.m.Lock()
	if w.ch != nil {
		w.requests[r.Id] = nil
		w.m.Unlock()
		w.ch <- r
		w.inc()
	} else {
		w.responses = append(w.responses, r)
		w.m.Unlock()
	}
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
	for i, req := range w.requests {
		if req != nil && req.Cancel() {
			w.requests[i] = nil
			w.incLocked()
		}
	}
}
