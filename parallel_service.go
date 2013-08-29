package iproto

import (
	"log"
	"sync"
	"time"
)

var _ = log.Print

type ParallelService struct {
	SimplePoint
	sync.Mutex
	f    func(*Request)
	sema chan bool
}

func NewParallelService(n int, timeout time.Duration, f func(*Request)) (serv *ParallelService) {
	if n == 0 {
		n = 1
	}
	serv = &ParallelService{
		SimplePoint: SimplePoint{
			Timeout: timeout,
		},
		f:    f,
		sema: make(chan bool, n),
	}
	serv.SimplePoint.Init(serv)
	for i := 0; i < n; i++ {
		serv.sema <- true
	}
	Run(serv)
	return
}

type parMiddle struct {
	BasicResponder
	serv *ParallelService
}

func (p *parMiddle) Respond(res Response) Response {
	p.serv.Lock()
	p.serv.sema <- true
	p.serv.Unlock()
	return res
}

func (serv *ParallelService) Loop() {
	var req *Request
	var ok bool
	var buf *[16]parMiddle
	var bufn int
Loop:
	for {
		select {
		case req, ok = <-serv.ReceiveChan():
			if !ok {
				break Loop
			}
		case <-serv.ExitChan():
			break Loop
		}

		if buf == nil {
			buf = &[16]parMiddle{}
		}

		mid := &buf[bufn]
		mid.serv = serv

		if bufn++; bufn == len(buf) {
			buf = nil
			bufn = 0
		}

		select {
		case <-serv.sema:
		case <-serv.ExitChan():
			req.Respond(RcShutdown, nil)
			break Loop
		}

		if req.SetInFly(mid) {
			req.SetTimeout(serv.Timeout)
			go serv.f(req)
		} else {
			serv.sema <- true
		}
	}

	for {
		if req, ok = <-serv.ReceiveChan(); !ok {
			break
		}
		req.Respond(RcShutdown, nil)
	}
}
