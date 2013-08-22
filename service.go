package iproto

import (
	"time"
	"log"
)

var _ = log.Print

type Service interface {
	// Send accepts request to work. It should setup deadline, if it is defined for end point
	Send(*Request)
	// SendWrapped accepts request to work. It should not setup deadline, assuming, someone did it already
	SendWrapped(*Request)
	Runned() bool
}

type FuncMiddleService func(*Request)
func (f FuncMiddleService) SendWrapped(r *Request) {
	f(r)
}

func (f FuncMiddleService) Send(r *Request) {
	f(r)
}

func (f FuncMiddleService) Runned() bool {
	return true
}

type FuncEndService func(*Request)
func (f FuncEndService) SendWrapped(r *Request) {
	if r.SetInFly(nil) {
		f(r)
	}
}

func (f FuncEndService) Send(r *Request) {
	if r.SetInFly(nil) {
		f(r)
	}
}

func (f FuncEndService) Runned() bool {
	return true
}

type EndPoint interface {
	Service
	Run(requests chan *Request, standalone bool)
	Stop()
}

func Run(s EndPoint) {
	if s.Runned() {
		log.Panicf("EndPoint already runned ( %v )", s)
	}
	ch := make(chan *Request, 16 * 1024)
	s.Run(ch, true)
}

//   SimplePoint is a simple EndPoint implementation.
//   One could start implementing by embedding it and overriding Run method and setting OnExit
//       type MyEndPoint struct {
//	       iproto.SimplePoint
//	       /* custom fields */
//       }
//       func (e *MyEndPoint) Init() {
//	       e.SimplePoint.OnExit = e.Exit
//	       e.SimplePoint.Init()
//       }
//       func (e *MyEndPoint) Run() {
//	       /* custom logick */
//       }
type SimplePoint struct {
	requests chan *Request
	exit     chan bool
	isStandalone  bool
	Timeout  time.Duration
	Worktime time.Duration
}
var _ EndPoint = (*SimplePoint)(nil)

func (s *SimplePoint) Runned() bool {
	return s.requests != nil
}

func (s *SimplePoint) SetChan(ch chan *Request, standalone bool) {
	s.requests = ch
	s.isStandalone = standalone
}

func (s *SimplePoint) ReceiveChan() <-chan *Request {
	return s.requests
}

func (s *SimplePoint) RunChild(p EndPoint) {
	if p.Runned() {
		log.Panicf("EndPoint already runned ( %v )", s)
	}
	p.Run(s.requests, false)
}

func (s *SimplePoint) Init() {
	s.exit = make(chan bool)
}

func (s *SimplePoint) ExitChan() <-chan bool {
	return s.exit
}

// Run - main function to override. Example
//     func (s *MyEndPoint) RunAsChild(ch chan *Request, standalone bool) {
//     	s.SetChan(ch, standalone)
//     	go func() {
//     		for {
//     			select {
//     			case req, ok := <-s.requests:
//     				if ok {
//     					if req.SetInFly() && !req.Expired() {
//     						s.doSomethingUseful(req)
//     					}
//     				} else {
//     					return
//     				}
//     			case <-s.exit:
//     				return
//     			}
//     		}
//     	}()
//     }
func (s *SimplePoint) Run(ch chan *Request, standalone bool) {
	s.SetChan(ch, standalone)
	panic("(*SimplePoint).Run should be overrided")
}

func (s *SimplePoint) SendWrapped(r *Request) {
	if s.requests == nil {
		panic("EndPoint is not running")
	}

	if !s.isStandalone {
		log.Panicf("you should not call SendWrapped on child endpoint %+v", s)
	}

	if !r.SetPending() {
		/* this could happen if SetDeadline already respond with timeout */
		if r.state == RsPerformed {
			return
		}
		log.Panicf("Request already sent somewhere %+v")
	}

	select {
	case s.requests <- r:
	case <-r.CancelChan():
	}
}

func (s *SimplePoint) Send(r *Request) {
	if s.requests == nil {
		panic("EndPoint is not running")
	}

	if !s.isStandalone {
		log.Panicf("you should not call SendWrapped on child endpoint %+v", s)
	}

	if !r.SetPending() {
		/* this could happen if SetDeadline already respond with timeout */
		if r.state == RsPerformed {
			return
		}
		log.Panicf("Request already sent somewhere %+v")
	}

	if s.Timeout > 0 {
		r.SetDeadline(NowEpoch().Add(s.Timeout), s.Worktime)
	}

	/* this could happen if SetDeadline already respond with timeout */
	if r.state == RsPerformed {
		return
	}

	select {
	case s.requests <- r:
	case <-r.CancelChan():
	}
}

func (s *SimplePoint) Stop() {
	s.exit <- true
}
