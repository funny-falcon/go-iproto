package iproto

import (
	"github.com/funny-falcon/go-iproto/util"
	"log"
)

var _ = log.Print

type CDeadline struct {
	basic BasicResponder
	state util.Atomic
	heap  heapRef
	send, recv uint32
}

const (
	dsNil  = util.Atomic(iota)
	dsCanceling
	dsResponding
)

func wrapInCDeadline(r *Request) {
	if r.Deadline.Zero() {
		return
	}
	d := CDeadline{}
	d.Wrap(r)
}

func (d *CDeadline) Wrap(r *Request) {
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

	d.heap = getHeap()
	d.heap.Insert(sendTimeout{d})
	d.heap.Insert(recvTimeout{d})
}

func (d *CDeadline) sendExpired() {
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

func (d *CDeadline) recvExpired() {
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

func (d *CDeadline) Respond(res Response) {
	d.state.Store(dsResponding)
	d.heap.Remove(sendTimeout{d})
	d.heap.Remove(recvTimeout{d})
	prev := d.basic.Unchain()
	if prev != nil {
		prev.Respond(res)
	}
}

func (d *CDeadline) Cancel() {
	heaps[d.heap].Remove(sendTimeout{d})
	heaps[d.heap].Remove(recvTimeout{d})
	if !d.state.Is(dsCanceling) {
		prev := d.basic.Unchain()
		if prev != nil {
			prev.Cancel()
		}
	}
}

type sendTimeout struct {
	*CDeadline
}

func (s sendTimeout) Value() int64 {
	d := s.CDeadline
	req := d.basic.Request
	return int64(req.Deadline) - int64(req.WorkTime)
}
func (s sendTimeout) Index() int {
	d := s.CDeadline
	return int(d.send)
}
func (s sendTimeout) SetIndex(i int) {
	d := s.CDeadline
	d.send = uint32(i)
}

func (s sendTimeout) Expire() {
	d := s.CDeadline
	d.sendExpired()
}

type recvTimeout struct {
	*CDeadline
}

func (s recvTimeout) Value() int64 {
	d := s.CDeadline
	req := d.basic.Request
	return int64(req.Deadline)
}
func (s recvTimeout) Index() int {
	d := s.CDeadline
	return int(d.recv)
}
func (s recvTimeout) SetIndex(i int) {
	d := s.CDeadline
	d.recv = uint32(i)
}

func (s recvTimeout) Expire() {
	d := s.CDeadline
	d.recvExpired()
}
