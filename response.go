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
        RcTemporary = RetCode(1)
        RcFatal    = RetCode(2)
        RcKindMask = RetCode(3)
)

const (
	RcShutdown = RcFatal | (0xff00)
	RcProtocolError = RcFatal | (0x0300)
)
const (
	RcTimeout = 0x0c00 | RcTemporary
	RcCanceled = 0xff00 | RcTemporary
	RcIOError = 0xfe00 | RcTemporary
)

type Response struct {
	Msg  RequestType
	Id   uint32
	Code RetCode
	Body Reader
}

func (res *Response) Valid() bool {
	return res.Code & RcKindMask == 0
}

func (res *Response) Restartable() bool {
	return res.Code & RcKindMask == RcTemporary
}

type Responder interface {
	Respond(*Response)
}

type Callback func(*Response)

func (f Callback) Respond(r *Response) {
	f(r)
}

type Chan chan *Response

func (ch Chan) Respond(r *Response) {
	ch <- r
}

type RequestMiddleware interface {
	Respond(*Response)
	setReq(req *Request, self RequestMiddleware)
	unchain() RequestMiddleware
}

type Middleware struct {
	Request *Request
	prev    RequestMiddleware
}

// Chain integrates Middleware into callback chain
func (r *Middleware) setReq(req *Request, self RequestMiddleware) {
	r.Request = req
	r.prev = req.chain
	req.chain = self
}

// Unchain removes Middleware from callback chain
func (r *Middleware) unchain() (prev RequestMiddleware) {
	prev = r.prev
	r.Request.chain = prev
	r.prev = nil
	r.Request = nil
	return
}

func (r *Middleware) Respond(resp *Response) {
}
