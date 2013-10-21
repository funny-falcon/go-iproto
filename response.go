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
	RcOK        = RetCode(0)
	RcTemporary = RetCode(1)
	RcFatal     = RetCode(2)
	RcInternal  = RetCode(3)
	RcKindMask  = RetCode(7)
)

const (
	RcShutdown      = RetCode(0xff03)
	RcProtocolError = RetCode(0x0302)
	RcInternalError = RetCode(0xfc02)
)
const (
	RcCanceled = RetCode(0xff03)
	RcIOError  = RetCode(0xfe03)
	RcTimeout  = RetCode(0xfd03)
)

type Response struct {
	Msg  RequestType
	Id   uint32
	Code RetCode
	Body Body
}

func (res *Response) Valid() bool {
	return res.Code&RcKindMask == 0
}

func (res *Response) Restartable() bool {
	return res.Code&RcKindMask == RcTemporary
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

type RequestBookmark interface {
	Respond(*Response)
	setReq(req *Request, self RequestBookmark)
	unchain() RequestBookmark
}

type Bookmark struct {
	Request *Request
	prev    RequestBookmark
}

// Chain integrates Bookmark into callback chain
func (r *Bookmark) setReq(req *Request, self RequestBookmark) {
	r.Request = req
	r.prev = req.chain
	req.chain = self
}

// Unchain removes Bookmark from callback chain
func (r *Bookmark) unchain() (prev RequestBookmark) {
	prev = r.prev
	r.Request.chain = prev
	r.prev = nil
	r.Request = nil
	return
}

func (r *Bookmark) Respond(resp *Response) {
}
