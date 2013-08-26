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
	chain     Middleware
	sync.Mutex
	timer     Timer
}

func (r *Request) SetDeadline(deadline Epoch, worktime time.Duration) {
	if !deadline.Zero() {
		d := Deadline{Deadline: deadline, WorkTime: worktime}
		d.Wrap(r)
	}
}

func (r *Request) SetTimeout(timeout time.Duration, worktime time.Duration) {
	if timeout > 0 {
		d := Deadline{Deadline: NowEpoch().Add(timeout), WorkTime: worktime}
		d.Wrap(r)
	}
}

func (r *Request) SetITimeout(timeout time.Duration) {
	if timeout > 0 && r.timer.E == nil {
		r.timer.E = r
		r.timer.After(timeout)
	}
}

//func (r *Request) sendExpired() {
func (r *Request) Expire() {
	state := atomic.LoadUint32(&r.state)
	for state == RsNew || state&(RsPending|RsInFly) != 0 {
		r.Lock()
		if state != r.state {
			state = r.state
			r.Unlock()
			continue
		}
		defer r.Unlock()
		r.chainCancel(nil)
		res := Response{Id: r.Id, Msg: r.Msg, Code: RcSendTimeout}
		r.Responder.Respond(res)
	}
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
func (r *Request) SetInFly(mid Middleware) (set bool) {
	if mid == nil {
		return r.cas(RsPending, RsInFly)
	} else {
		r.Lock()
		if r.state == RsPending {
			r.state = RsInFly
			r.chainMiddleware(mid)
			set = true
		}
		r.Unlock()
	}
	return
}

func (r *Request) Cancel() bool {
	r.Lock()
	defer r.Unlock()
	if r.state == RsNew || r.state&(RsPending|RsInFly) != 0 {
		r.chainCancel(nil)
		return true
	}
	return false
}

func (r *Request) ResponseInAMiddle(middle Middleware, res Response) {
	r.chainCancel(middle)
	if middle != nil && (r.state == RsCanceled || r.chain != middle) {
		log.Panicf("Try to respond in a middle for response %+v, while it were not in a chain", middle)
	}
	if middle != nil {
		r.unchainMiddleware(middle)
	}
	r.chainResponse(res)
}

/* Canceled returns: did some called Cancel() and it were successful.
   Note, that Canceler callback could be not called yet at this point of time.
   Also, only positive answer is trustful, since some could call Cancel() just
   after this function returns
*/
func (r *Request) Canceled() bool {
	st := atomic.LoadUint32(&r.state)
	return st == RsToCancel || st == RsCanceled
}

// ResetToPending is for ResendeRs on IOError. It should be called in a Responder.
// Note, if it returns false, then Responder is already performed
func (r *Request) ResetToPending(res Response, originalResponder Responder) bool {
	if r.state == RsPrepared {
		r.state = RsPending
		return true
	}
	log.Panicf("ResetToPending should be called only for performed requests")
	return false
}

func (r *Request) chainCancel(middle Middleware) {
	r.state = RsToCancel
	for chain := r.chain; chain != nil; {
		chain.Cancel()
		if chain == middle {
			return
		}
		chain = r.unchainMiddleware(chain)
	}
	r.state = RsCanceled
	r.timer.Stop()
}

func (r *Request) chainResponse(res Response) {
	r.state = RsPrepared
	for chain := r.chain; chain != nil; {
		res = chain.Respond(res)
		if r.state != RsPrepared {
			return
		}
		chain = r.unchainMiddleware(chain)
	}
	r.Responder.Respond(res)
	r.state = RsPerformed
	r.Responder = nil
	r.Body = nil
	r.timer.Stop()
}

func (r *Request) Response(res Response, responder Middleware) {
	r.Lock()
	defer r.Unlock()
	if r.state == RsInFly && (responder == nil || responder == r.chain) {
		r.chainResponse(res)
	}
}

func (r *Request) ChainMiddleware(res Middleware) (chained bool) {
	r.Lock()
	if r.state == RsNew || r.state == RsPending {
		chained = true
		r.chainMiddleware(res)
	}
	r.Unlock()
	return
}

func (r *Request) chainMiddleware(res Middleware) {
	res.setReq(r, res)
}

func (r *Request) UnchainMiddleware(res Middleware) {
	r.Lock()
	if r.chain == res {
		r.unchainMiddleware(res)
	}
	r.Unlock()
}

func (r *Request) unchainMiddleware(res Middleware) (next Middleware) {
	next = res.unchain()
	return
}

func (r *Request) Respond(code RetCode, body []byte) {
	r.Response(Response{Id: r.Id, Msg: r.Msg, Code: code, Body: body}, nil)
}

const (
	RsNew     = uint32(0)
	RsPending = uint32(1 << iota)
	RsInFly
	RsPrepared
	RsPerformed
	RsToCancel
	RsCanceled
	RsTimeout
)
