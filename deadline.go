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
	r.Lock()
	defer r.Unlock()
	if d.Request == r {
		state := r.State()
		if state == RsNew || state == RsPending {
			res := Response{Id: r.Id, Msg: r.Msg, Code: RcSendTimeout}
			r.ResponseInAMiddle(d, res)
		} else if state == RsPrepared || state == RsPerformed {
			return
		} else if state == RsInFly && d.WorkTime == 0 {
			d.doRecvExpired()
		} else if recvRemains := d.Deadline.Sub(NowEpoch()); recvRemains <= 0 {
			d.doRecvExpired()
		} else {
			d.timer = time.AfterFunc(recvRemains, d.recvExpired)
		}
	}
}

func (d *Deadline) doRecvExpired() {
	r := d.Request
	res := Response{Id: r.Id, Msg: r.Msg, Code: RcRecvTimeout}
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

func (d *Deadline) Respond(res Response) Response {
	d.timer.Stop()
	return res
}

func (d *Deadline) Cancel() {
	if d.timer != nil {
		d.timer.Stop()
	}
}
