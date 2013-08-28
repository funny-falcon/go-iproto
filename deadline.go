package iproto

import (
	"github.com/funny-falcon/go-iproto/util"
	"log"
	"time"
)

var _ = log.Print

type Deadline struct {
	BasicResponder

	Deadline Epoch
	/* WorkTime is a hint to EndPoint, will Deadline be reached if we send request now.
	   If set, then sender will check: if Deadline - TypicalWorkTime is already expired,
	   than it will not try to send Request to network */
	WorkTime time.Duration

	timer *time.Timer
}

const (
	dsNil = util.Atomic(iota)
	dsCanceling
	dsResponding
)

func (d *Deadline) Wrap(r *Request) {
	sendRemains := d.Deadline.Sub(NowEpoch()) - d.WorkTime
	if !r.ChainMiddleware(d) {
		return
	}

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
	d.timer = nil
	if d.Request == r {
		state := r.State()
		if state & RsNotWaiting == 0 {
			r.Respond(RcTimeout, nil)
		} else if state == RsInFly {
			if d.WorkTime > 0 {
				recvRemains := d.Deadline.Sub(NowEpoch())
				if recvRemains > 0 {
					d.timer = time.AfterFunc(recvRemains, d.recvExpired)
					return
				}
			}
			r.Respond(RcTimeout, nil)
		}
	}
}

func (d *Deadline) recvExpired() {
	r := d.Request
	if r == nil {
		return
	}
	d.timer = nil
	r.Respond(RcTimeout, nil)
}

func (d *Deadline) Respond(res Response) Response {
	if t := d.timer; t != nil {
		d.timer.Stop()
	}
	return res
}
