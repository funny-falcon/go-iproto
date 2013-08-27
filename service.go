package iproto

import (
	"log"
	"time"
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
	if r.SetPending() && r.SetInFly(nil) {
		f(r)
	}
}

func (f FuncEndService) Send(r *Request) {
	if r.SetPending() && r.SetInFly(nil) {
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
	ch := make(chan *Request, 1024)
	s.Run(ch, true)
}

type PointLoop interface {
	Loop()
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
	b Buffer
	exit         chan bool
	standalone bool
	PointLoop
	Timeout      time.Duration
	Worktime     time.Duration
}

var _ EndPoint = (*SimplePoint)(nil)

func (s *SimplePoint) Runned() bool {
	return s.b.in != nil
}

func (s *SimplePoint) Run(ch chan *Request, standalone bool) {
	s.b.in = ch
	s.standalone = standalone
	if standalone {
		s.b.out = make(chan *Request, 16*1024)
		s.b.onExit = func(){ close(s.b.out) }
		s.standalone = standalone
		go s.b.loop()
	}
	go s.Loop()
}

func (s *SimplePoint) ReceiveChan() <-chan *Request {
	if s.standalone {
		return s.b.out
	} else {
		return s.b.in
	}
}

func (s *SimplePoint) RunChild(p EndPoint) {
	if p.Runned() {
		log.Panicf("EndPoint already runned ( %v )", s)
	}
	if s.standalone {
		p.Run(s.b.out, false)
	} else {
		p.Run(s.b.in, false)
	}
}

func (s *SimplePoint) Init(p PointLoop) {
	s.PointLoop = p
	s.exit = make(chan bool)
}

func (s *SimplePoint) ExitChan() <-chan bool {
	return s.exit
}

func (s *SimplePoint) SendWrapped(r *Request) {
	if s.b.in == nil {
		panic("EndPoint is not running")
	}

	if !s.standalone {
		log.Panicf("you should not call SendWrapped on child endpoint %+v", s)
	}

	if !r.SetPending() {
		/* this could happen if SetDeadline already respond with timeout */
		if r.state == RsPerformed || r.state == RsCanceled {
			return
		}
		log.Panicf("Request already sent somewhere %+v")
	}

	s.b.in <- r
}

func (s *SimplePoint) Send(r *Request) {
	if s.b.in == nil {
		panic("EndPoint is not running")
	}

	if !s.standalone {
		log.Panicf("you should not call SendWrapped on child endpoint %+v", s)
	}

	if !r.SetPending() {
		/* this could happen if SetDeadline already respond with timeout */
		if r.state == RsPerformed || r.state == RsCanceled {
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

	s.b.in <- r
}

func (s *SimplePoint) Stop() {
	s.exit <- true
}
