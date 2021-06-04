package iproto

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/funny-falcon/go-iproto/marshal"
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
}

type Request struct {
	Msg       RequestType
	Id        uint32
	state     uint32
	Body      Body
	Response  *Response
	Responder Responder
	chain     RequestBookmark
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

func (r *Request) ShutDown() {
	r.RespondFail(RcShutdown)
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
func (r *Request) SetInFly(mid RequestBookmark) (set bool) {
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

func (r *Request) IsInFly() (set bool) {
	return atomic.LoadUint32(&r.state) == RsInFly
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
	log.Panicf("ResetToNew should be called only for performed requests")
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

func (r *Request) Respond(code RetCode, val interface{}) {
	var body []byte
	if o, ok := val.(Body); ok {
		body = o
	} else {
		body = marshal.Write(val)
	}
	r.RespondBytes(code, body)
}

func (r *Request) RespondBytes(code RetCode, body []byte) {
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

func (r *Request) ChainBookmark(res RequestBookmark) (chained bool) {
	r.Lock()
	if r.state == RsNew || r.state == RsPending {
		chained = true
		res.setReq(r, res)
	}
	r.Unlock()
	return
}

type ReqContext struct {
	Bookmark
	Context
}

func (cm *ReqContext) Respond(res *Response) {
	if res.Code == RcCanceled {
		cm.Cancel()
	} else if res.Code == RcTimeout {
		cm.Expire()
		cm.Request.ResetToPending()
		cm.Request.SetInFly(nil)
	} else {
		cm.Context.Done()
	}
}

func (cm *ReqContext) Done() {
	if req := cm.Request; req != nil {
		req.RespondFail(RcInternalError)
	}
	cm.Context.Done()
}

func (r *Request) Context() (cx *ReqContext) {
	cx = &ReqContext{}
	if !r.SetInFly(cx) {
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

func SendMsgBody(serv Service, m RequestType, r interface{}) (*Request, Chan) {
	var body []byte
	var ok bool
	if body, ok = r.(Body); !ok {
		body = marshal.Write(r)
	}
	res := make(Chan, 1)
	req := &Request{Msg: m, Body: body, Responder: res}
	serv.Send(req)
	return req, res
}

func Send(serv Service, r RequestData) (*Request, Chan) {
	return SendMsgBody(serv, r.IMsg(), r)
}

func CallMsgBody(serv Service, m RequestType, r interface{}) *Response {
	var body []byte
	var ok bool
	if body, ok = r.(Body); !ok {
		body = marshal.Write(r)
	}
	res := make(Chan, 1)
	serv.Send(&Request{Msg: m, Body: body, Responder: res})
	return <-res
}

func Call(serv Service, r RequestData) *Response {
	return CallMsgBody(serv, r.IMsg(), r)
}

type Body []byte

func (b Body) IWrite(w *marshal.Writer) {
	w.Bytes([]byte(b))
}

func (b Body) Reader() (r marshal.Reader) {
	r.Body = b
	return
}

func (b Body) Read2() (r marshal.Reader, err error) {
	r.Body = b
	err = r.Read(b)
	return
}

func (b Body) ReadTail2() (r marshal.Reader, err error) {
	r.Body = b
	err = r.ReadTail(b)
	return
}

func (b Body) Read(i interface{}) error {
	return marshal.Read(b, i)
}

func (b Body) ReadTail(i interface{}) error {
	return marshal.ReadTail(b, i)
}
