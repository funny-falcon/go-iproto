package iproto

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)
var _ = log.Print

const (
	wgInFly = 0
	wgFailed = wgCancel | wgExpire
)
const (
	wgChan = (1 << iota)
	wgWait
	wgCancel
	wgExpire
	wgFailPerformed
)

const (
	wgBufSize = 16
)

type WaitGroup struct {
	m         sync.Mutex
	c, r      uint32
	requests  []*[wgBufSize]Request
	responses []Response
	ch        chan Response
	w         *sync.Cond
	timer     Timer
	timerSet  bool
	kind      int32
}

func (w *WaitGroup) Slice(n int) (r []byte) {
	return
}

func (w *WaitGroup) TimeoutFrom(d Service) {
	w.SetTimeout(d.DefaultTimeout())
}

func (w *WaitGroup) SetTimeout(timeout time.Duration) {
	if timeout > 0 && !w.timerSet {
		w.timerSet = true
		w.timer.E = w
		w.timer.After(timeout)
	}
}

func (w *WaitGroup) Request(msg RequestType, body []byte) *Request {
	if w.r%wgBufSize == 0 {
		w.requests = append(w.requests, &[wgBufSize]Request{})
	}

	req := &(*w.requests[w.r/wgBufSize])[w.r%wgBufSize]
	*req = Request{
		Id:        w.r,
		Msg:       msg,
		Body:      body,
		Responder: w,
		timerSet:  w.timerSet,
	}
	atomic.AddUint32(&w.r, 1)
	if w.kind & wgFailed != 0 {
		w.performFail(req)
	}
	return req
}

func (w *WaitGroup) Each() <-chan Response {
	if w.kind & wgFailed != 0 {
		w.performFailAll()
	}
	w.m.Lock()
	w.kind |= wgChan
	w.ch = make(chan Response, w.r)

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

func (w *WaitGroup) Results() []Response {
	if w.kind & wgFailed != 0 {
		w.performFailAll()
	}
	w.m.Lock()
	w.kind |= wgWait
	w.w = sync.NewCond(&w.m)
	if cap(w.responses) < int(w.r) {
		tmp := make([]Response, len(w.responses), w.r)
		copy(tmp, w.responses)
		w.responses = tmp
	}
	if atomic.LoadUint32(&w.c) < w.r {
		w.w.Wait()
	}
	res := w.responses
	w.responses = nil
	w.m.Unlock()
	return res
}

func (w *WaitGroup) Respond(r Response) {
	if w.ch == nil {
		w.m.Lock()
		if w.ch == nil {
			w.responses = append(w.responses, r)
			if w.c++; w.c == w.r && w.kind & wgWait != 0 {
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
	}
}

func (w *WaitGroup) Cancel() {
	if w.c == w.r {
		return
	}
	w.m.Lock()
	w.kind |= wgCancel
	w.m.Unlock()
}

func (w *WaitGroup) performFailAll() {
	r := int(w.r)
	for row, reqs := range w.requests {
		if row > (r-1) / wgBufSize {
			break
		}
		for i := range reqs {
			if row * wgBufSize + i < r {
				w.performFail(&reqs[i])
			}
		}
	}
	w.kind |= wgFailPerformed
}

func (w *WaitGroup) performFail(r *Request) {
	switch {
	case w.kind&wgCancel != 0:
		r.Cancel()
	case w.kind&wgExpire != 0:
		r.Expire()
	}
}

func (w *WaitGroup) Expire() {
	if w.c == w.r {
		return
	}
	w.m.Lock()
	w.kind |= wgExpire
	w.m.Unlock()
}
