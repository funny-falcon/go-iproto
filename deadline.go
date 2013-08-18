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
	if r := d.Request; r != nil {
		if r.expireSend() {
			d.state.Store(dsCanceling)
			r.doCancel()
			res := Response { Id: r.Id, Msg: r.Msg, Code: RcSendTimeout }
			if prev := d.Unchain(); prev != nil {
				prev.Respond(res)
			}
		} else {
			if r.WorkTime > 0 {
				if recvRemains := r.Deadline.Sub(NowEpoch()); recvRemains > 0 {
					d.timer = time.AfterFunc(recvRemains, d.recvExpired)
				}
			}
			d.recvExpired()
		}
	}
}

func (d *Deadline) recvExpired() {
	if r := d.Request; r != nil && r.goingToCancel() {
		d.state.Store(dsCanceling)
		r.doCancel()
		res := Response { Id: r.Id, Msg: r.Msg, Code: RcRecvTimeout }
		if prev := d.Unchain(); prev != nil {
			prev.Respond(res)
		}
	}
}

func (d *Deadline) Respond(res Response) {
	if d.state.CAS(dsNil, dsResponding) {
		d.timer.Stop()
		prev := d.Unchain()
		if prev != nil {
			prev.Respond(res)
		}
	}
}

func (d *Deadline) Cancel() {
	if !d.state.Is(dsCanceling) {
		d.timer.Stop()
		prev := d.Unchain()
		if prev != nil {
			prev.Cancel()
		}
	}
}
