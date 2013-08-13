package iproto

import (
	"github.com/funny-falcon/go-iproto/util"
)

type Canceler interface {
	Cancel()
}

type Request struct {
	Msg      RequestType
	Id       uint32
	Body     []byte
	Callback Callback
	Canceler Canceler
	Deadline Deadline
	state    util.Atomic
}

func (r *Request) InitLinkCopy(cb Callback, req *Request) {
	r.state = rsPending
	r.Callback = cb
	req.Canceler = r
}

func (r *Request) Cancel() {
	if set, ok := r.GoingToCancel(); ok {
		if set != rsPerformedToCancel {
			if r.Canceler != nil && set != rsPreparedToCancel {
				r.Canceler.Cancel()
			} else {
				r.state.Store(rsCanceled)
				r.ResponseCancel()
			}
		}
	}
	return
}

func (r *Request) GoingToCancel() (util.Atomic, bool) {
	var old, set util.Atomic
	for {
		old = r.state.Get()
		if old == rsNew || old == rsPending || old == rsInFly {
			set = rsToCancel
		} else if old == rsPrepared {
			set = rsPreparedToCancel
		} else if old == rsPerformed {
			set = rsPerformedToCancel
		} else {
			return 0, false
		}
		if r.state.CAS(old, set) {
			return set, true
		}
	}
}

func (r *Request) SetCanceled() bool {
	old := r.state.Get()
	if old == rsToCancel || old == rsPreparedToCancel {
		return r.state.CAS(old, rsCanceled)
	}
	return false
}

func (r *Request) SetRetryCanceled() bool {
	return r.state.CAS(rsPerformedToCancel, rsCanceled)
}

func (r *Request) Canceled() bool {
	return r.state.Is(rsCanceled)
}

func (r *Request) SetPending() bool {
	return r.state.CAS(rsNew, rsPending)
}

func (r *Request) SetInFly() bool {
	return r.state.CAS(rsPending, rsInFly)
}

func (r *Request) SetPrepared() bool {
	return r.state.CAS(rsInFly, rsPrepared)
}

func (r *Request) SetPerformed() bool {
	return r.state.CAS(rsPrepared, rsPerformed)
}

// ResetToPending is for Resenders on IOError
func (r *Request) ResetToPending() bool {
	var old util.Atomic
	for {
		if old == rsPerformed {
			if r.state.CAS(old, rsPending) {
				return true
			}
		} else if old == rsPerformedToCancel {
			if r.state.CAS(old, rsCanceled) {
				r.ResponseCancel()
				return false
			}
		} else {
			return false
		}
	}
}

func (r *Request) Response(res Response) {
	r.SetPending()
	r.SetInFly()
	if r.SetPrepared() && r.SetPerformed() {
		r.Canceler = nil
		if res.Code == RcCanceled {
			r.state.Store(rsCanceled)
		}
		r.Callback.Response(res)
	}
}

func (r *Request) ResponseCancel() {
	r.Canceler = nil
	res := Response{Msg: r.Msg, Id: r.Id, Code: RcCanceled}
	r.Callback.Response(res)
}

func (r *Request) Performed() bool {
	return r.state.Is(rsPerformed)
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
	rsPerformedToCancel
)
