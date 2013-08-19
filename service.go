package iproto

import (
	"time"
	"log"
)

var _ = log.Print

type EndPoint interface {
	Run(chan *Request)
	Stop()
	RequestChan() chan<- *Request
	DefaultDeadline() Epoch
	TypicalWorkTime(RequestType) time.Duration
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
}
var _ EndPoint = (*SimplePoint)(nil)

func (s *SimplePoint) SetChan(ch chan *Request) {
	s.requests = ch
}

func (s *SimplePoint) RequestChan() chan<- *Request {
	return s.requests
}

func (s *SimplePoint) ReceiveChan() <-chan *Request {
	return s.requests
}

func (s *SimplePoint) RunChild(p EndPoint) {
	p.Run(s.requests)
}

func (s *SimplePoint) DefaultDeadline() (d Epoch) {
	return
}

func (s *SimplePoint) TypicalWorkTime(RequestType) (d time.Duration) {
	return
}

func (s *SimplePoint) Init() {
	s.exit = make(chan bool)
}

func (s *SimplePoint) ExitChan() <-chan bool {
	return s.exit
}

// Run - main function to override. Example
//     func (s *MyEndPoint) Run(ch chan *Request) {
//     	s.SetChan(ch)
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
func (s *SimplePoint) Run(ch chan *Request) {
	panic("(*SimplePoint).Run should be overrided")
}

func (s *SimplePoint) Stop() {
	s.exit <- true
}
