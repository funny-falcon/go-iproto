package iproto

// RetCode is a iproto return code, which lays in first bytes of response
type RetCode uint32

// Response return codes
// RcOK - good answer
// RcTimeout - response where timeouted by ServiceWithDeadline
// RcShortBody - response with body shorter, than return code
// RcIOError - socket were disconnected before answere arrives
// RcCanceled - ...
const (
	RcOK       = RetCode(0)
	RcShutdown = ^RetCode(0) - iota
	RcProtocolError
	RcFailed
	RcFatalError = RcShutdown - 255 - iota
	RcSendTimeout
	RcRecvTimeout
	RcIOError
	RcRestartable = RcShutdown - 512
	RcInvalid     = RcRestartable
)

type Response struct {
	Msg  RequestType
	Id   uint32
	Code RetCode
	Body []byte
}

func (res *Response) Valid() bool {
	return res.Code < RcInvalid
}

func (res *Response) Restartable() bool {
	return res.Code < RcFatalError
}

type Responder interface {
	Respond(Response)
}

type Callback func(Response)

func (f Callback) Respond(r Response) {
	f(r)
}

type Middleware interface {
	Respond(Response) Response
	Cancel()
	valid() bool
	setReq(req *Request, self Middleware)
	unchain() Middleware
	previous() Middleware
}

type CancelChaner interface {
	CancelChan() chan bool
	InitChan()
	CloseChan()
}

type BasicResponder struct {
	Request *Request
	prev    Middleware
}

// Chain integrates BasicResponder into callback chain
func (r *BasicResponder) setReq(req *Request, self Middleware) {
	r.Request = req
	r.prev = req.chain
	req.chain = self
}

// Unchain removes BasicResponder from callback chain
func (r *BasicResponder) unchain() (prev Middleware) {
	prev = r.prev
	r.Request.chain = prev
	r.prev = nil
	r.Request = nil
	return
}

func (r *BasicResponder) previous() Middleware {
	return r.prev
}

func (r *BasicResponder) valid() bool {
	return r.Request != nil
}

func (r *BasicResponder) Respond(resp Response) Response {
	return resp
}

func (r *BasicResponder) Cancel() {
}

type BasicCancelChaner struct {
	ch chan bool
}

func (c *BasicCancelChaner) InitChan() {
	c.ch = make(chan bool)
}

func (c *BasicCancelChaner) CancelChan() chan bool {
	return c.ch
}

func (c *BasicCancelChaner) CloseChan() {
	close(c.ch)
}
