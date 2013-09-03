package iproto

import (
	"log"
	"sync"
	"time"
)

var _ = log.Print

type BF struct {
	N       int
	Timeout time.Duration
}

func (b BF) New(f func(*Context)) (serv *ParallelService) {
	if b.N == 0 {
		b.N = 1
	}
	serv = &ParallelService{
		SimplePoint: SimplePoint{
			Timeout: b.Timeout,
		},
		f:    f,
		sema: make(chan bool, b.N),
	}
	serv.SimplePoint.Init(serv)
	for i := 0; i < b.N; i++ {
		serv.sema <- true
	}
	Run(serv)
	return
}

type ParallelService struct {
	SimplePoint
	sync.Mutex
	f    func(*Context)
	sema chan bool
}

type parMiddle struct {
	Middleware
	serv *ParallelService
}

func (p *parMiddle) Respond(res *Response) {
	p.serv.Lock()
	p.serv.sema <- true
	p.serv.Unlock()
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

		if req.ChainMiddleware(mid) {
			if ctx := req.Context(); ctx != nil {
				go serv.f(ctx)
			}
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
