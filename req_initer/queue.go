package req_initer

import (
	"github.com/funny-falcon/go-iproto"
	"time"
)

type Queue struct {
	send, recv heap
}

func NewQueue() (q *Queue) {
	q = new(Queue)
	q.Init()
	return
}

func (q *Queue) Init() {
	q.send = heap{heap: make([]*Request, 0, 16), kind: _send}
	q.recv = heap{heap: make([]*Request, 0, 16), kind: _recv}
}

func (q *Queue) Add(r *Request) {
	r.Deadline.Check()
	q.send.Add(r)
	q.recv.Add(r)
}

func (q *Queue) Remains(now iproto.Epoch) time.Duration {
	var send, recv time.Duration = 1<<62, 1<<62
	sendReq := q.send.First()
	recvReq := q.recv.First()
	if sendReq != nil {
		send = time.Duration(sendReq.Deadline.Send - now)
	}
	if recvReq != nil {
		recv = time.Duration(recvReq.Deadline.Receive - now)
	}
	if send < recv {
		return send
	} else {
		return recv
	}
}

func (q *Queue) ToSend() (r *Request) {
	return q.send.First()
}

func (q *Queue) HasToRecv() bool {
	return q.recv.Len() > 0
}

func (q *Queue) RemoveSend(r *Request) {
	q.send.Remove(r)
}

func (q *Queue) RemoveRecv(r *Request) {
	q.recv.Remove(r)
}

func (q *Queue) ExpiredSend(now iproto.Epoch) (r *Request) {
	req := q.send.First()
	if req != nil && req.Deadline.Send < now {
		return req
	}
	return nil
}

func (q *Queue) ExpiredRecv(now iproto.Epoch) (r *Request) {
	req := q.recv.First()
	if req != nil && req.Deadline.Receive < now {
		return req
	}
	return nil
}
