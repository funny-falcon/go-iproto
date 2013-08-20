package iproto

import (
	"sync"
)

type WaitGroup struct {
	m sync.Mutex
	wg sync.WaitGroup
	callback Callback
	requests []*Request
	responses []Response
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
	w.wg.Add(1)
	w.m.Unlock()
	return req
}

func (w *WaitGroup) Wait(f func(Response)) {
	w.m.Lock()
	w.callback = f
	for _, resp := range w.responses {
		w.requests[resp.Id] = nil
		f(resp)
		w.wg.Done()
	}
	w.responses = nil
	w.m.Unlock()
	w.wg.Wait()
}

func (w *WaitGroup) Respond(r Response) {
	w.m.Lock()
	defer w.m.Unlock()
	if w.callback != nil {
		w.callback(r)
		w.requests[r.Id] = nil
		w.wg.Done()
	} else {
		w.responses = append(w.responses, r)
	}
}

func (w *WaitGroup) Cancel() {
	w.m.Lock()
	defer w.m.Unlock()
	for i, req := range w.requests {
		if req != nil && req.Cancel() {
			w.requests[i] = nil
			w.wg.Done()
		}
	}
}
