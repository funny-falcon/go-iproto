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
	reqn      uint32
	requests  []*[wgBufSize]Request
	responses []Response
	ch        chan Response
	w         *sync.Cond
	kind      int32
	timer     Timer
}

func (w *WaitGroup) Init() {
}

func (w *WaitGroup) SetITimeout(timeout time.Duration) {
	if timeout > 0 && w.timer.E == nil {
		w.timer.E = w
		w.timer.After(timeout)
	}
}

func (w *WaitGroup) Request(msg RequestType, body []byte) *Request {
	w.lock()
	if w.reqn%wgBufSize == 0 {
		w.requests = append(w.requests, &[wgBufSize]Request{})
	}

	req := &(*w.requests[w.reqn/wgBufSize])[w.reqn%wgBufSize]
	*req = Request{
		Id:        uint32(w.reqn),
		Msg:       msg,
		Body:      body,
		Responder: w,
	}
	w.reqn++
	w.unlock()
	return req
}

func (w *WaitGroup) Each() <-chan Response {
	w.lock()
	w.kind = wgChan
	w.ch = make(chan Response, w.reqn)

	for _, resp := range w.responses {
		w.requests[resp.Id] = nil
		w.ch <- resp
	}
	w.responses = nil
	if uint32(w.c) == w.reqn {
		close(w.ch)
	}
	w.unlock()
	return w.ch
}

func (w *WaitGroup) Results() []Response {
	w.lock()
	w.kind = wgWait
	w.w = sync.NewCond(&w.m)
	if cap(w.responses) < int(w.reqn) {
		tmp := make([]Response, len(w.responses), w.reqn)
		copy(tmp, w.responses)
		w.responses = tmp
	}
	if uint32(w.c.Get()) < w.reqn {
		w.w.Wait()
	}
	res := w.responses
	w.responses = nil
	w.unlock()
	return res
}

func (w *WaitGroup) Respond(r Response) {
	if w.ch == nil {
		w.lock()
		if w.ch == nil {
			w.responses = append(w.responses, r)
			w.incLocked()
			w.unlock()
			return
		}
		w.unlock()
	}
	w.ch <- r
	w.inc()
}

func (w *WaitGroup) inc() {
	if v := w.c.Incr(); uint32(v) == w.reqn {
		w.lock()
		switch w.kind {
		case wgChan:
			w.timer.Stop()
			close(w.ch)
		case wgWait:
			w.timer.Stop()
			w.w.Signal()
		}
		w.unlock()
	}
}

func (w *WaitGroup) incLocked() {
	if v := w.c.Incr(); uint32(v) == w.reqn {
		switch w.kind {
		case wgChan:
			w.timer.Stop()
			close(w.ch)
		case wgWait:
			w.timer.Stop()
			w.w.Signal()
		}
	}
}

func (w *WaitGroup) Cancel() {
	if uint32(w.c) == w.reqn {
		return
	}
	w.lock()
	defer w.unlock()

	w.timer.Stop()
	for _, reqs := range w.requests {
		for i := range reqs {
			req := &reqs[i]
			if req.state != RsNew && req.Cancel() {
				w.incLocked()
			}
		}
	}
}

func (w *WaitGroup) Expire() {
	w.timer.Stop()

	requests := make([]*Request, 0, w.reqn)
	w.lock()
	n := w.reqn
	for _, reqs := range w.requests {
		if n == 0 {
			break
		}
		for i := range reqs {
			if n == 0 {
				break
			}
			req := &reqs[i]
			state := req.state
			if req.Responder == w && state == RsNew || state&(RsPending|RsInFly) != 0 {
				requests = append(requests, req)
			}
			n--
		}
	}
	w.unlock()
	for _, req := range requests {
		if req != nil {
			req.Expire()
		}
	}
}

func (w *WaitGroup) lock() {
	w.m.Lock()
}

func (w *WaitGroup) unlock() {
	w.m.Unlock()
}
