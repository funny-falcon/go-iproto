package iproto

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// RequestType is a iproto request tag which goes fiRst in a packet
type RequestType uint32

const (
	Ping = RequestType(0xFF00)
)

const (
	PingRequestId = ^uint32(0)
)

type Request struct {
	Msg       RequestType
	Id        uint32
	state     uint32
	Body      []byte
	Responder Responder
	chain     RequestMiddleware
	sync.Mutex
	timer    Timer
	timerSet bool
}

func (r *Request) SetTimeout(timeout time.Duration) {
	if timeout > 0 && !r.timerSet {
		r.timerSet = true
		r.timer.E = r
		r.timer.After(timeout)
	}
}

func (r *Request) Expire() {
	r.respondFail(RcTimeout)
}

func (r *Request) Cancel() {
	r.respondFail(RcCanceled)
}

func (r *Request) State() uint32 {
	return r.state
}

func (r *Request) cas(old, new uint32) (set bool) {
	set = atomic.CompareAndSwapUint32(&r.state, old, new)
	return
}

func (r *Request) SetPending() (set bool) {
	return r.cas(RsNew, RsPending)
}

func (r *Request) IsPending() (set bool) {
	return atomic.LoadUint32(&r.state) == RsPending
}

// SetInFly should be called when you going to work with request.
func (r *Request) SetInFly(mid RequestMiddleware) (set bool) {
	if mid == nil {
		return r.cas(RsPending, RsInFly)
	} else {
		r.Lock()
		if r.state == RsPending {
			r.state = RsInFly
			mid.setReq(r, mid)
			set = true
		}
		r.Unlock()
	}
	return
}

func (r *Request) Performed() bool {
	st := atomic.LoadUint32(&r.state)
	return st == RsPrepared || st == RsPerformed
}

func (r *Request) Canceled() bool {
	return r.Performed()
}

// ResetToPending is for ResendeRs on IOError. It should be called in a Responder.
// Note, if it returns false, then Responder is already performed
func (r *Request) ResetToPending() bool {
	if r.state == RsPrepared {
		r.state = RsPending
		return true
	}
	log.Panicf("ResetToPending should be called only for performed requests")
	return false
}

func (r *Request) chainResponse(res Response) {
	r.state = RsPrepared
	for chain := r.chain; chain != nil; {
		res = chain.Respond(res)
		if r.state != RsPrepared {
			return
		}
		chain = chain.unchain()
	}
	r.Responder.Respond(res)
	r.state = RsPerformed
	r.Responder = nil
	r.Body = nil
	r.timer.Stop()
}

func (r *Request) Response(res Response) {
	r.Lock()
	if r.state == RsInFly {
		r.chainResponse(res)
	}
	r.Unlock()
}

func (r *Request) Respond(code RetCode, body []byte) {
	r.Response(Response{Id: r.Id, Msg: r.Msg, Code: code, Body: body})
}

func (r *Request) respondFail(code RetCode) {
	r.Lock()
	if r.state&RsPerforming == 0 {
		res := Response{Id: r.Id, Msg: r.Msg, Code: code}
		r.chainResponse(res)
	}
	r.Unlock()
}

func (r *Request) ChainMiddleware(res RequestMiddleware) (chained bool) {
	r.Lock()
	if r.state == RsNew || r.state == RsPending {
		chained = true
		res.setReq(r, res)
	}
	r.Unlock()
	return
}

func (r *Request) Context() (cx *Context) {
	cx = new(Context)
	mid := &cxAsMid{cx: cx}
	cx.cxAsMid = mid
	if !r.SetInFly(mid) {
		return nil
	}
	return
}

const (
	RsNew        = uint32(0)
	RsNotWaiting = ^(RsNew | RsPending)
	RsPerforming = RsPrepared | RsPerformed
)
const (
	RsPending = uint32(1 << iota)
	RsInFly
	RsPrepared
	RsPerformed
)
