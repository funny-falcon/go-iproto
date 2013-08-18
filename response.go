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
	RcOK          = RetCode(0)
	RcShutdown = ^RetCode(0) - iota
	RcProtocolError
	RcFatalError = RcShutdown - 255 - iota
	RcSendTimeout
	RcRecvTimeout
	RcIOError
	RcRestartable = RcShutdown - 512
	RcInvalid = RcRestartable
)

type Response struct {
	Msg RequestType
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
	Cancel()
}

type ChainingResponder interface {
	Responder
	SetReq(req *Request, self Responder)
	Unchain() Responder
}

type BasicResponder struct {
	Request *Request
	prev Responder
}

// Chain integrates BasicResponder into callback chain
func (r *BasicResponder) SetReq(req *Request, self Responder) {
	r.Request = req
	r.prev = req.Responder
	req.Responder = self
}

// Unchain removes BasicResponder from callback chain
func (r *BasicResponder) Unchain() (prev Responder) {
	if req := r.Request; req != nil {
		prev = r.prev
		req.Responder = prev
		r.prev = nil
		r.Request = nil
	}
	return
}

func (r *BasicResponder) Respond(res Response) {
	prev := r.Unchain()
	if prev != nil {
		prev.Respond(res)
	}
}

func (r *BasicResponder) Cancel() {
	prev := r.Unchain()
	if prev != nil {
		prev.Cancel()
	}
}

func (r *BasicResponder) SetInFly() bool {
	if ireq := r.Request; ireq != nil {
		return ireq.SetInFly()
	}
	return false
}

type Callback struct {
	cb func(Response)
}
