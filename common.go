package iproto

import "errors"

// RequestType is a iproto request tag which goes first in a packet
type RequestType uint32

const (
	Ping = RequestType(0xFF00)
)

const (
	PingRequestId = ^uint32(0)
)

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
	RcCanceled = ^RetCode(0) - iota
	RcShutDown
	RcShortBody
	RcFatalError = RcCanceled - 255 - iota
	RcSendTimeout
	RcRecvTimeout
	RcIOError
	RcInvalid = RcCanceled - 512
)

var Canceled = errors.New("Request canceled by initiator")

type Callback interface {
	/* Response invoked on every event, res.Code will be set adequately.
	   Sorry for error missage, but duplication looks ugly
	*/
	Response(res Response)
}

type Response struct {
	Msg RequestType
	/* If Id == 0, it will be ignored.
	   If you're performing client request, it is better to leave it zeroed.
	   Otherwise, service will try to preserve Id.
	   You should not set Id == PingRequestId, it is for internal usage only.
	*/
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
