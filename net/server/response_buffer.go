package server

import (
	"github.com/funny-falcon/go-iproto/net"
	"log"
)

type ResponseBuffer struct {
	in, out chan net.Response
	buf []net.Response
	exit <-chan bool
}

func (rb *ResponseBuffer) Init() {
	rb.in = make(chan net.Response, 8)
	rb.out = make(chan net.Response, 8)
	rb.buf = make([]net.Response, 0, 16)
	go rb.Loop()
}

func (rb *ResponseBuffer) CloseIn() {
	close(rb.in)
}

func (rb *ResponseBuffer) Loop() {
	defer rb.recover()
	in := rb.in
	for {
		if len(rb.buf) > 0 {
			cur := rb.buf[0]
			select {
			case resp, ok := <-in:
				if ok {
					rb.buf = append(rb.buf, resp)
				} else {
					in = nil
				}
			case rb.out <- cur:
				rb.buf = rb.buf[1:]
			}
		} else if in != nil {
			select {
			case cur, ok := <-in:
				if ok {
					select {
					case rb.out <- cur:
					default:
						rb.buf = append(rb.buf, cur)
					}
				} else {
					in = nil
				}
			}
		} else {
			break
		}
	}
	close(rb.out)
}

func (rb *ResponseBuffer) recover() {
	if err := recover(); err != nil {
		// usually we are here if we try to put in a closed rb.out
		log.Printf("Server Connection ResponseBuffer panic: %+v", err)
	}
}

