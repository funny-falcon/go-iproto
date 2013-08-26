package iproto

import (
	"log"
)
var _ = log.Print

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
			req := first.Request
			if req == nil || !req.UnchainMiddleware(first) {
				continue Loop
			}
			for {
				if !req.IsPending() {
					continue Loop
				}
				select {
				case r, ok := <-b.in:
					if ok {
						b.push(r)
					} else {
						b.in = nil
					}
				case b.out <- req:
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
	r.ChainMiddleware(&(*buf)[len(*buf)-1])
}

func (b *Buffer) shift() (br *BasicResponder, ok bool) {
	if len(b.buf) > 0 {
		buf := &b.buf[0]
		if len(*buf) > 0 {
			br, ok = &(*buf)[0], true
			*buf = (*buf)[1:]
			if cap(*buf) == 0 {
				b.buf = b.buf[1:]
			}
		}
	}
	return
}
