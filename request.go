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

type RequestData interface {
	IMsg() RequestType
	IWriter
}

type Request struct {
	Msg       RequestType
	Id        uint32
	state     uint32
	Body      Body
	Response  *Response
	Responder Responder
	chain     RequestMiddleware
	sync.Mutex
	timer    Timer
	timerSet bool
}

func (r *Request) SetTimeout(timeout time.Duration) {
	if timeout > 0 && !r.timerSet {
		r.timerSet = true
		r.timer.After(timeout, r)
	}
}

func (r *Request) Timer() *Timer {
	return &r.timer
}

func (r *Request) Expire() {
	r.RespondFail(RcTimeout)
}

func (r *Request) Cancel() {
	r.RespondFail(RcCanceled)
}

func (r *Request) IOError() {
	r.RespondFail(RcIOError)
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

func (r *Request) ResetToNew() bool {
	if r.state == RsPrepared {
		r.state = RsNew
		return true
	}
	log.Panicf("ResetToPending should be called only for performed requests")
	return false
}

func (r *Request) chainResponse(code RetCode, body []byte) {
	if r.Response == nil {
		r.Response = &Response{Id: r.Id, Msg: r.Msg}
	} else {
		r.Response.Id = r.Id
		r.Response.Msg = r.Msg
	}
	r.Response.Code = code
	r.Response.Body = body
	res := r.Response

	r.state = RsPrepared
	for chain := r.chain; chain != nil; {
		chain.Respond(res)
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

func (r *Request) Respond(code RetCode, body []byte) {
	r.Lock()
	if r.state == RsInFly {
		r.chainResponse(code, body)
	}
	r.Unlock()
}

func (r *Request) RespondFail(code RetCode) {
	r.Lock()
	if r.state&RsPerforming == 0 {
		r.chainResponse(code, nil)
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

const rrsize = 32

type RGenerator struct {
	req *[rrsize]Request
	res *[rrsize]Response
	w   Writer
	i   int32
}

func (gen *RGenerator) Request(id uint32, msg RequestType, val IWriter) (req *Request) {
	var res *Response
	if gen.req == nil {
		gen.req = &[rrsize]Request{}
		gen.res = &[rrsize]Response{}
	}
	req = &gen.req[gen.i]
	res = &gen.res[gen.i]
	req.Response = res
	if gen.i++; gen.i == rrsize {
		gen.i = 0
		gen.req = nil
		gen.res = nil
	}
	req.Id = id
	req.Msg = msg
	var ok bool
	if req.Body, ok = val.(Body); !ok {
		val.IWrite(val, &gen.w)
		req.Body = gen.w.Written()
	}
	return
}

var gencache = make(chan *RGenerator, 1024)

func GetGenerator() (gen *RGenerator) {
	select {
	case gen = <-gencache:
	default:
		gen = &RGenerator{}
	}
	return
}

func PutGenerator(gen *RGenerator) {
	if gen != nil {
		select {
		case gencache <- gen:
		default:
		}
	}
}
