package req_initer

import (
	"github.com/funny-falcon/go-iproto"
	//"log"
	//"sync/atomic"
	//"unsafe"
)

type Request struct {
	iproto.Request
	origin  *iproto.Request
	sendInd int
	recvInd int
}

func (r *Request) before(o *Request, kind heapKind) bool {
	switch kind {
	case _send:
		return r.Deadline.Send < o.Deadline.Send
	case _recv:
		return r.Deadline.Receive < o.Deadline.Receive
	default:
		panic("unknown heap kind")
	}
}

func (r *Request) index(kind heapKind) int {
	switch kind {
	case _send:
		return r.sendInd - 1
	case _recv:
		return r.recvInd - 1
	default:
		panic("unknown heap kind")
	}
}

func (r *Request) setIndex(ind int, kind heapKind) {
	switch kind {
	case _send:
		r.sendInd = ind + 1
	case _recv:
		r.recvInd = ind + 1
	default:
		panic("unknown heap kind")
	}
}

func (r *Request) sendTimeouted(holder *ReqIniter) {
	res := iproto.Response{Msg: r.Msg, Id: r.Id, Code: iproto.RcSendTimeout}
	//log.Printf("SEND TIMEOUT %+v", r)
	if r.origin != nil {
		r.origin.Response(res)
		r.origin = nil
	}
}

func (r *Request) recvTimeouted(holder *ReqIniter) {
	res := iproto.Response{Msg: r.Msg, Id: r.Id, Code: iproto.RcRecvTimeout}
	//log.Printf("RECV TIMEOUT %+v", r)
	r.origin.Response(res)
	r.origin = nil
}
