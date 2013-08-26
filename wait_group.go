package iproto

import (
	"github.com/funny-falcon/go-iproto/util"
	"log"
	"sync"
)

const (
	wgInFly = iota
	wgChan
	wgWait
)

const (
	wgBufSize = 16
)

type WaitGroup struct {
	m         sync.Mutex
	c         util.Atomic
	reqn      int32
	requests  []*[wgBufSize]Request
	responses []Response
	cancel    chan bool
	ch        chan Response
	w         sync.Mutex
	kind      int32
}

func (w *WaitGroup) Init() {
	w.cancel = make(chan bool)
}

func (w *WaitGroup) Request(msg RequestType, body []byte) *Request {
	w.m.Lock()
	if w.reqn%wgBufSize == 0 {
		w.requests = append(w.requests, &[wgBufSize]Request{})
	}

	req := &(*w.requests[w.reqn/wgBufSize])[w.reqn%wgBufSize]
	*req = Request{
		Id:        uint32(len(w.requests)),
		Msg:       msg,
		Body:      body,
		Responder: w,
	}
	w.reqn++
	w.m.Unlock()
	return req
}

func (w *WaitGroup) Each() <-chan Response {
	w.m.Lock()
	w.kind = wgChan
	w.ch = make(chan Response, w.reqn)
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
	if cap(w.responses) < int(w.reqn) {
		tmp := make([]Response, len(w.responses), w.reqn)
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
	if v := w.c.Incr(); int32(v) == w.reqn {
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
	if w.c++; int32(w.c) == w.reqn {
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
	for _, reqs := range w.requests {
		for i := range reqs {
			req := &reqs[i]
			if req.state != RsNew && req.Cancel() {
				w.incLocked()
			}
		}
	}
}

