package req_initer

import (
	"errors"
	"github.com/funny-falcon/go-iproto"
	"github.com/funny-falcon/go-iproto/util"
	"log"
	"time"
)

const (
	RINew = uint32(iota)
	RIInited
	RIRunning
	RIStopped
)

type ReqIniter struct {
	Name     string
	timeout  iproto.Timeout
	incoming chan *iproto.Request
	outgoing chan *iproto.Request
	responses chan iproto.Response

	state util.Synced
	exit  chan bool

	reqHolder util.IdGenerator
	queue     Queue

	endPoints map[string]iproto.EndPoint
}

var _ iproto.EndPoint = (*ReqIniter)(nil)

func (r *ReqIniter) DefaultTimeout() iproto.Timeout {
	return r.timeout
}

func (r *ReqIniter) DefaultDeadline() iproto.Deadline {
	return r.timeout.NowDeadline()
}

func (r *ReqIniter) RequestChan() chan<- *iproto.Request {
	return r.incoming
}

func (r *ReqIniter) IProtoStop() {
	r.exit <- true
}

var NotInitedYet = errors.New("ReqIniter is not inited yet")
var AlreadyInited = errors.New("ReqIniter already inited")
var AlreadyStopped = errors.New("ReqIniter already stopped")

func (r *ReqIniter) AddEndPoint(name string, end iproto.EndPoint) error {
	r.state.Lock()
	defer r.state.Unlock()

	if r.state.Is(RINew) {
		return NotInitedYet
	} else if r.state.Is(RIStopped) {
		return AlreadyStopped
	}

	r.endPoints[name] = end

	if r.state.Is(RIRunning) {
		end.IProtoRun(r.outgoing)
	}
	return nil
}

func (r *ReqIniter) Init(timeout iproto.Timeout) (err error) {
	r.state.Lock()
	defer r.state.Unlock()
	if !r.state.CAS(RINew, RIInited) {
		return AlreadyInited
	}
	r.timeout = timeout.SetDefaults()
	r.outgoing = make(chan *iproto.Request, 10240)
	r.responses = make(chan iproto.Response, 10240)
	r.endPoints = make(map[string]iproto.EndPoint)
	r.exit = make(chan bool)
	r.reqHolder.DefInit()
	r.queue.Init()
	r.state.Store(RIInited)
	return
}

func (r *ReqIniter) stop() {
	r.state.Lock()
	defer func() {
		r.state.Store(RIStopped)
		r.state.Unlock()
	}()
	for _, end := range r.endPoints {
		end.IProtoStop()
	}
	r.endPoints = nil
}

func (r *ReqIniter) IProtoRun(ch chan *iproto.Request) {
	r.state.Lock()
	if r.incoming != nil {
		r.state.Unlock()
		log.Panicf("ReqIniter ReRun")
	}
	r.incoming = ch
	for _, end := range r.endPoints {
		end.IProtoRun(r.outgoing)
	}
	r.state.Store(RIRunning)
	r.state.Unlock()

	go r.Loop()
}

func (r *ReqIniter) Loop() {
	timer := time.NewTimer(r.queue.Remains(iproto.NowEpoch()))
Loop:
	for {
		var cur *Request

		select {
		case _ = <-r.exit:
			break Loop
		default:
		}

		cur = r.queue.ToSend()
		remains := r.queue.Remains(iproto.NowEpoch())
		timer.Reset(remains)

		if cur != nil {
			select {
			case <-r.exit:
				break Loop
			case <-timer.C:
				r.cleanExpired(timer)
			case r.outgoing <- &cur.Request:
				r.queue.RemoveSend(cur)
			case incoming, ok := <-r.incoming:
				if ok {
					r.add(incoming)
				} else {
					break Loop
				}
			case response := <-r.responses:
				r.response(response)
			}
		} else {
			select {
			case <-r.exit:
				break Loop
			case <-timer.C:
				r.cleanExpired(timer)
			case incoming, ok := <-r.incoming:
				if ok {
					r.add(incoming)
				} else {
					break Loop
				}
			case response := <-r.responses:
				r.response(response)
			}
		}
	}

	for req := r.queue.ToSend(); req != nil; req = r.queue.ToSend() {
		resp := iproto.Response{
			Msg: req.Msg,
			Id: req.Id,
			Code: iproto.RcShutDown,
		}
		r.response(resp)
	}

	for r.queue.HasToRecv() {
		var cur *Request

		cur = r.queue.ToSend()
		timer.Reset(r.queue.Remains(iproto.NowEpoch()))

		if cur != nil {
			select {
			case <-timer.C:
				r.cleanExpired(timer)
			case r.outgoing <- &cur.Request:
				r.queue.RemoveSend(cur)
			case response := <-r.responses:
				r.response(response)
			}
		} else {
			select {
			case <-timer.C:
				r.cleanExpired(timer)
			case response := <-r.responses:
				r.response(response)
			}
		}
	}
}

func (r *ReqIniter) cleanExpired(timer *time.Timer) {
	var req *Request
	now := iproto.NowEpoch()
	for {
		if req = r.queue.ExpiredSend(now); req == nil {
			break
		}
		req.sendTimeouted(r)
		r.remove(req)
	}
	for {
		if req = r.queue.ExpiredRecv(now); req == nil {
			break
		}
		r.queue.RemoveRecv(req)
		req.recvTimeouted(r)
	}
	timer.Reset(r.queue.Remains(now))
}

func (r *ReqIniter) remove(req *Request) {
	r.queue.RemoveSend(req)
	r.queue.RemoveRecv(req)
	r.reqHolder.Remove(req.Id)
}

func (r *ReqIniter) Response(res iproto.Response) {
	r.responses <- res
}

func (r *ReqIniter) response(res iproto.Response) {
	void := r.reqHolder.Remove(res.Id)
	if void == nil {
		return
	}
	req := void.(*Request)
	r.queue.RemoveSend(req)
	r.queue.RemoveRecv(req)
	if req.origin != nil {
		res.Id = req.origin.Id
		req.origin.Response(res)
		req.origin = nil
	}
}

func (r *ReqIniter) add(request *iproto.Request) {
	if !request.SetInFly() {
		return
	}
	req := &Request{
		Request: *request,
		origin:  request,
	}
	req.InitLinkCopy(r, request)
	var err error
	if req.Id, err = r.reqHolder.Next(); err != nil {
		log.Panicf("Could not reserve Id: %v", err)
	}
	r.reqHolder.Set(req.Id, req)
	r.queue.Add(req)
}
