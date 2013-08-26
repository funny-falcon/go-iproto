package iproto

import (
	"log"
	"sync"
)

var _ = log.Print

type ParallelService struct {
	SimplePoint
	sync.Mutex
	f	func(*Request)
	sema     chan bool
}

func NewParallelService(n int, f func(*Request)) (serv *ParallelService) {
	if n == 0 {
		n = 1
	}
	serv = &ParallelService{
		f:	f,
		sema:     make(chan bool, n),
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

func (p* parMiddle) Respond(res Response) Response {
	p.serv.Lock()
	p.serv.sema<- true
	p.serv.Unlock()
	return res
}

func (p* parMiddle) Cancel() {
	p.serv.Lock()
	p.serv.sema<- true
	p.serv.Unlock()
}

func (serv *ParallelService) Loop() {
	var buf *[16]parMiddle
	var bufn int
Loop:
	for {
		var req *Request
		var ok bool
		if req, ok = <-serv.ReceiveChan(); !ok {
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

		<-serv.sema
		if req.SetInFly(mid) {
			go serv.f(req)
		} else {
			serv.sema <- true
		}
	}
}

