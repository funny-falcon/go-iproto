package iproto

import (
	"log"
)

type BufferPoint struct {
	b Buffer
	standalone bool
	S EndPoint
}

func (b *BufferPoint) Send(r *Request) {
	if !r.SetPending() {
		/* this could happen if SetDeadline already respond with timeout */
		if r.state == RsPerformed || r.state == RsCanceled {
			return
		}
		log.Panicf("Request already sent somewhere %+v")
	}

	b.b.in <- r
}

func (b *BufferPoint) SendWrapped(r *Request) {
	if !r.SetPending() {
		/* this could happen if SetDeadline already respond with timeout */
		if r.state == RsPerformed || r.state == RsCanceled {
			return
		}
		log.Panicf("Request already sent somewhere %+v")
	}

	b.b.in <- r
}

func (b *BufferPoint) Runned() bool {
	return b.b.in != nil
}

func (b *BufferPoint) Run(reqs chan *Request, standalone bool) {
	b.b.in = reqs
	b.b.out = make(chan *Request, 16*1024)
	b.standalone = standalone
	b.S.Run(b.b.out, false)
	go b.b.loop()
}

func (b *BufferPoint) Stop() {
	if b.standalone {
		close(b.b.in)
	}
	b.b.in = nil
}

var _ EndPoint = (*BufferPoint)(nil)

type Buffer struct {
	in chan *Request
	out chan *Request
	onExit func()
	buf   [][]BasicResponder
}

func (b *Buffer) loop() {
Loop:
	for {
		if first, ok := b.shift(); ok {
			for {
				if first.Request == nil || !first.Request.IsPending() {
					continue Loop
				}
				select {
				case r, ok := <-b.in:
					if ok {
						b.push(r)
					} else {
						b.in = nil
					}
				case b.out <- first.Request:
					continue Loop
				}
			}
		}
		for {
			var in chan *Request
			if in = b.in; in == nil {
				break
			}
			if r, ok := <-in; ok {
				select {
				case b.out<-r:
				default:
					b.push(r)
					continue Loop
				}
			} else {
				break Loop
			}
		}
	}
	if b.onExit != nil {
		b.onExit()
	}
}

func (b *Buffer) push(r *Request) {
	var buf *[]BasicResponder
	l := len(b.buf)
	if l > 0 {
		buf = &b.buf[l-1]
	}
	if buf == nil || len(*buf) == cap(*buf) {
		b.buf = append(b.buf, make([]BasicResponder, 0, 16))
		buf = &b.buf[l]
	}
	*buf = append(*buf, BasicResponder{})
	r.chainMiddleware(&(*buf)[len(*buf)-1])
}

func (b *Buffer) shift() (br BasicResponder, ok bool) {
	if len(b.buf) > 0 {
		buf := &b.buf[0]
		if len(*buf) > 0 {
			br, ok = (*buf)[0], true
			*buf = (*buf)[1:]
			if cap(*buf) == 0 {
				b.buf = b.buf[1:]
			}
		}
	}
	return
}
