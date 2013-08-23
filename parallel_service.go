package iproto

import (
	"github.com/funny-falcon/go-iproto/util"
	"sync"
)

type ParallelMiddleware struct {
	BasicResponder
	serv       *ParallelService
	prev, next *ParallelMiddleware
	performing util.Atomic
}

const (
	parInit = iota
	parInFly
	parFinished
)

func (p *ParallelMiddleware) Respond(res Response) Response {
	p.Cancel()
	return res
}

func (p *ParallelMiddleware) Cancel() {
	for {
		if p.performing.CAS(parInFly, parFinished) {
			p.serv.sema <- true
			break
		}
		if p.performing.CAS(parInit, parFinished) {
			p.serv.Lock()
			if p.prev != nil {
				p.prev.next = p.next
				p.next.prev = p.prev
			}
			p.serv.Unlock()
			break
		}
	}
}

type ParallelService struct {
	sync.Mutex
	work     Service
	runned   bool
	appended chan bool
	list     ParallelMiddleware
	sema     chan bool
}

func NewParallelService(n int, work Service) (serv *ParallelService) {
	if n == 0 {
		n = 1
	}
	serv = &ParallelService{
		work:     work,
		runned:   true,
		appended: make(chan bool, 1),
		sema:     make(chan bool, n),
	}
	serv.list.next = &serv.list
	serv.list.prev = &serv.list
	for i := 0; i < n; i++ {
		serv.sema <- true
	}
	go serv.loop()
	return
}

func (serv *ParallelService) Runned() bool {
	return serv.runned
}

func (serv *ParallelService) SendWrapped(r *Request) {
	serv.Lock()
	defer serv.Unlock()

	if serv.appended == nil {
		r.Respond(RcShutdown, nil)
		return
	}
	middle := &ParallelMiddleware{
		serv: serv,
		prev: serv.list.prev,
		next: &serv.list,
	}
	serv.list.prev.next = middle
	serv.list.prev = middle
	if !r.ChainMiddleware(middle) {
		return
	}
	select {
	case serv.appended <- true:
	default:
	}
}

func (serv *ParallelService) Send(r *Request) {
	serv.SendWrapped(r)
}

func (serv *ParallelService) loop() {
Loop:
	for {
		select {
		case app := <-serv.appended:
			if !app {
				serv.appended = nil
			}
			continue Loop
		case <-serv.sema:
		}

		if !serv.runOne() {
			serv.sema <- true
			if serv.list.next == &serv.list && serv.appended == nil {
				break Loop
			}
			if app := <-serv.appended; !app {
				serv.appended = nil
			}
		}
	}
}

func (serv *ParallelService) runOne() (runned bool) {
	serv.Lock()
	defer serv.Unlock()

	next := serv.list.next
	if next == &serv.list {
		return false
	}

	next.prev.next = next.next
	next.next.prev = next.prev
	next.prev = nil
	next.next = nil
	request := next.Request
	if !next.performing.CAS(parInit, parInFly) {
		return false
	}
	go serv.work.Send(request)
	return true
}
