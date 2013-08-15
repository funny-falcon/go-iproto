package iproto

import (
	"time"
	"github.com/funny-falcon/go-iproto/util"
	"log"
)

var _ = log.Print

type Deadline struct {
	basic BasicResponder
	send *time.Timer
	recv *time.Timer
	state util.Atomic
}

const (
	dsNil  = util.Atomic(iota)
	dsCanceling
	dsResponding
)

func expireSend(r *Request) {
}

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

	now := NowEpoch()
	recvRemains := r.Deadline.Sub(now)
	sendRemains := recvRemains - r.WorkTime
	d.basic.Chain(r)

	if sendRemains < 0 {
		d.sendExpired()
		return
	}

	d.send = time.AfterFunc(sendRemains, d.sendExpired)
	d.recv = time.AfterFunc(recvRemains, d.recvExpired)
}

func (d *Deadline) sendExpired() {
	r := d.basic.Request
	if r != nil && r.expireSend() {
		d.state.Store(dsCanceling)
		r.doCancel()
		res := Response { Id: r.Id, Msg: r.Msg, Code: RcSendTimeout }
		if prev := d.basic.Unchain(); prev != nil {
			prev.Respond(res)
		}
	}
}

func (d *Deadline) recvExpired() {
	r := d.basic.Request
	if r != nil && r.goingToCancel() {
		d.state.Store(dsCanceling)
		r.doCancel()
		res := Response { Id: r.Id, Msg: r.Msg, Code: RcRecvTimeout }
		if prev := d.basic.Unchain(); prev != nil {
			prev.Respond(res)
		}
	}
}

func (d *Deadline) Respond(res Response) {
	d.state.Store(dsResponding)
	d.send.Stop()
	d.recv.Stop()
	prev := d.basic.Unchain()
	if prev != nil {
		prev.Respond(res)
	}
}

func (d *Deadline) Cancel() {
	log.Print("Deadline cancel")
	d.send.Stop()
	d.recv.Stop()
	if !d.state.Is(dsCanceling) {
		log.Print("Deadline cancel but not canceling")
		prev := d.basic.Unchain()
		if prev != nil {
			prev.Cancel()
		}
	}
}
