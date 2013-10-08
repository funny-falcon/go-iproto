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

func (b BF) New(f func(*Context, *Request) (RetCode, interface{})) (serv *ParallelService) {
	if b.N == 0 {
		b.N = 1
	}
	serv = &ParallelService{
		SimplePoint: SimplePoint{
			Timeout: b.Timeout,
		},
		f:    f,
		sema: make(chan struct{}, b.N),
	}
	serv.SimplePoint.Init(serv)
	for i := 0; i < b.N; i++ {
		serv.sema <- struct{}{}
	}
	Run(serv)
	return
}

type ParallelService struct {
	SimplePoint
	sync.Mutex
	f    func(*Context, *Request) (RetCode, interface{})
	sema chan struct{}
}

func (serv *ParallelService) Loop() {
	var req *Request
	var ok bool
Loop:
	for {
		select {
		case <-serv.ExitChan():
			break Loop
		default:
			select {
			case req, ok = <-serv.ReceiveChan():
				if !ok {
					break Loop
				}
			default:
				select {
				case req, ok = <-serv.ReceiveChan():
					if !ok {
						break Loop
					}
				case <-serv.ExitChan():
					break Loop
				}
			}
		}

		select {
		case <-serv.ExitChan():
			req.ShutDown()
			break Loop
		default:
			select {
			case <-serv.sema:
			default:
				select {
				case <-serv.sema:
				case <-serv.ExitChan():
					req.ShutDown()
					break Loop
				}
			}
		}

		if ctx := req.Context(); ctx != nil {
			go serv.serv(ctx)
		} else {
			serv.sema <- struct{}{}
		}
	}

	for {
		if req, ok = <-serv.ReceiveChan(); !ok {
			break
		}
		req.Respond(RcShutdown, nil)
	}
}

func (serv *ParallelService) inc(ctx *ReqContext) {
	ctx.Done()
	serv.sema <- struct{}{}
}

func (serv *ParallelService) serv(ctx *ReqContext) {
	defer serv.inc(ctx)
	if req := ctx.Request; req != nil {
		req.Respond(serv.f(&ctx.Context, req))
	}
}
