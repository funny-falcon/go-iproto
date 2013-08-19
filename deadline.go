package iproto

import (
	"github.com/funny-falcon/go-iproto/util"
	"log"
	"time"
)

var _ = log.Print

type Deadline struct {
	BasicResponder
	state util.Atomic
	timer  *time.Timer
}

const (
	dsNil  = util.Atomic(iota)
	dsCanceling
	dsResponding
)

func wrapInDeadline(r *Request) {
	if r.Deadline.Zero() {
		return
	}
	d := Deadline{}
	d.Wrap(r)
}

func (d *Deadline) Wrap(r *Request) {
	if r.Deadline.Zero() {
		return
	}

	r.canceled = make(chan bool, 1)

	sendRemains := r.Deadline.Sub(NowEpoch()) - r.WorkTime
	r.ChainResponder(d)

	if sendRemains < 0 {
		d.sendExpired()
		return
	}

	d.timer = time.AfterFunc(sendRemains, d.sendExpired)
}

func (d *Deadline) sendExpired() {
	r := d.Request
	if r == nil {
		return
	}
	r.Lock()
	defer r.Unlock()
	if d.Request == r {
		state := r.State()
		if state == RsNew || state == RsPending {
			res := Response { Id: r.Id, Msg: r.Msg, Code: RcSendTimeout }
			r.ResponseInAMiddle(d, res)
		} else if state == RsPrepared || state == RsPerformed {
			return
		} else if state == RsInFly && r.WorkTime == 0 {
			d.doRecvExpired()
		} else if recvRemains := r.Deadline.Sub(NowEpoch()); recvRemains <= 0 {
			d.doRecvExpired()
		} else {
			d.timer = time.AfterFunc(recvRemains, d.recvExpired)
		}
	}
}

func (d *Deadline) doRecvExpired() {
	r := d.Request
	res := Response { Id: r.Id, Msg: r.Msg, Code: RcRecvTimeout }
	r.ResponseInAMiddle(d, res)
}

func (d *Deadline) recvExpired() {
	r := d.Request
	if r == nil {
		return
	}
	r.Lock()
	defer r.Unlock()
	if d.Request == r {
		state := r.State()
		if state == RsNew || state == RsPending || state == RsInFly {
			d.doRecvExpired()
		}
	}
}

func (d *Deadline) Respond(res *Response) {
	d.timer.Stop()
}

func (d *Deadline) Cancel() {
	if d.timer != nil {
		d.timer.Stop()
	}
}
