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
	Run(requests chan *Request)
	Stop()
}

func Run(s EndPoint) {
	if s.Runned() {
		log.Panicf("EndPoint already runned ( %v )", s)
	}
	s.Run(nil)
}

type PointLoop interface {
	Loop()
}

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
	return s.b.ch != nil
}

func (s *SimplePoint) Run(ch chan *Request) {
	if ch == nil {
		ch = make(chan *Request, 16 * 1024)
		s.standalone = true
		s.b.init()
	}
	s.b.ch = ch
	if s.standalone {
		go s.b.loop()
	}
	go s.Loop()
}

func (s *SimplePoint) ReceiveChan() <-chan *Request {
	return s.b.ch
}

func (s *SimplePoint) RunChild(p EndPoint) {
	if p.Runned() {
		log.Panicf("EndPoint already runned ( %v )", s)
	}
	p.Run(s.b.ch)
}

func (s *SimplePoint) Init(p PointLoop) {
	s.PointLoop = p
	s.exit = make(chan bool)
}

func (s *SimplePoint) ExitChan() <-chan bool {
	return s.exit
}

func (s *SimplePoint) SendWrapped(r *Request) {
	if s.b.ch == nil {
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

	s.b.push(r)
}

func (s *SimplePoint) Send(r *Request) {
	if s.b.ch == nil {
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

	s.b.push(r)
}

func (s *SimplePoint) Stop() {
	if s.standalone {
		s.b.close()
	}
	s.exit <- true
}
