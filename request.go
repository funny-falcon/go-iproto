package iproto

import (
	"github.com/funny-falcon/go-iproto/util"
	"log"
	"time"
)

// RequestType is a iproto request tag which goes first in a packet
type RequestType uint32

const (
	Ping = RequestType(0xFF00)
)

const (
	PingRequestId = ^uint32(0)
)

type Request struct {
	Msg      RequestType
	Id       uint32
	state    util.Atomic
	Body     []byte
	Responder Responder

	Deadline     Epoch
	/* WorkTime is a hint to EndPoint, will Deadline be reached if we send request now.
	   If set, then sender will check: if Deadline - TypicalWorkTime is already expired,
	   than it will not try to send Request to network */
	WorkTime time.Duration

	canceled  chan bool
}

func (r *Request) Send(serv EndPoint) bool {
	if !r.SetPending() {
		log.Panic("Request already sent somewhere")
		return false
	}
	if r.Deadline.Zero() {
		r.Deadline = serv.DefaultDeadline()
	}
	if r.WorkTime == 0 {
		r.WorkTime = serv.TypicalWorkTime(r.Msg)
	}

	wrapInDeadline(r)

	select {
	case serv.RequestChan() <- r:
		return true
	case <-r.canceled:
		return false
	}
}

func (r *Request) SendExpired() bool {
	return r.Deadline.WillExpire(r.WorkTime)
}

func (r *Request) Expired() bool {
	return r.Deadline.WillExpire(0)
}

func (r *Request) CancelChan() <-chan bool {
	return r.canceled
}

func (r *Request) SetPending() bool {
	return r.state.CAS(rsNew, rsPending)
}

// SetInFly should be called when you going to work with request.
func (r *Request) SetInFly(res ChainingResponder) bool {
	if r.state.CAS(rsPending, rsInFly) {
		res.SetReq(r, res)
		return true
	}
	return false
}

func (r *Request) setPrepared() bool {
	return r.state.CAS(rsInFly, rsPrepared)
}

func (r *Request) setPerformed() bool {
	if r.state.CAS(rsPrepared, rsPerformed) {
		return true
	}
	return r.state.CAS(rsPreparedIgnoreCancel, rsPerformed)
}

func (r *Request) doCancel() {
	if r.Responder != nil {
		r.Responder.Cancel()
	}
	select {
	case r.canceled <- true:
	default:
	}
	r.state.Store(rsCanceled)
	r.Responder = nil
}

func (r *Request) Cancel() bool {
	if r.goingToCancel() {
		r.doCancel()
		return true
	}
	return false
}

func (r *Request) expireSend() bool {
	return r.state.CAS(rsPending, rsToCancel)
}

func (r *Request) goingToCancel() (willCancel bool) {
	var old, set util.Atomic
	for {
		old, set = r.state.Get(), 0
		willCancel = false
		if old == rsNew || old == rsPending || old == rsInFly {
			set = rsToCancel
			willCancel = true
		} else if old == rsPrepared {
			set = rsPreparedIgnoreCancel
		} else {
			return
		}
		if r.state.CAS(old, set) {
			return
		}
	}
}

/* Canceled returns: did some called Cancel() and it were successful.
   Note, that Canceler callback could be not called yet at this point of time.
   Also, only positive answer is trustful, since some could call Cancel() just
   after this function returns
   */
func (r *Request) Canceled() bool {
	st := r.state.Get()
	return st == rsToCancel || st == rsPreparedToCancel || st == rsCanceled
}

// ResetToPending is for Resenders on IOError. It should be called in a Responder.
// Note, if it returns false, then Responder is already performed
func (r *Request) ResetToPending(res Response, originalResponder Responder) bool {
	if r.state.CAS(rsPrepared, rsPending) {
		return true
	}
	if r.state.Is(rsPreparedIgnoreCancel) {
		originalResponder.Respond(res)
		return false
	}
	log.Panicf("ResetToPending should be called only for performed requests")
	return false
}

func (r *Request) Response(res Response) (respondCalled bool) {
	if r.setPrepared() {
		r.Responder.Respond(res)
		r.setPerformed()
		return true
	}
	return false
}

func (r *Request) ResponseRecvTimeout() (respondCalled bool) {
	if r.setPrepared() {
		res := Response { Id: r.Id, Msg: r.Msg, Code: RcRecvTimeout }
		r.Responder.Respond(res)
		r.setPerformed()
		return true
	}
	return false
}

func (r *Request) ResponseIOError() (respondCalled bool) {
	if r.setPrepared() {
		res := Response { Id: r.Id, Msg: r.Msg, Code: RcIOError }
		r.Responder.Respond(res)
		r.setPerformed()
		return false
	}
	return true
}

func (r *Request) ChainResponder(res ChainingResponder) {
	res.SetReq(r, res)
}

const (
	rsNew = util.Atomic(iota)
	rsPending
	rsInFly
	rsPrepared
	rsPerformed
	rsToCancel
	rsCanceled
	rsPreparedToCancel
	rsPreparedIgnoreCancel
)
