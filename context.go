package iproto

import (
	"sync"
)

const (
	cxReqBuf = 32
	cxBodyBuf = 256
)

type contextMiddleware struct {
	Middleware
	c *Context
}

func (cm *contextMiddleware) Respond(res Response) Response {
	if c := cm.c; c != nil {
		c.removeCanceler(cm)
	}
	return res
}

func (cm *contextMiddleware) Cancel() {
	if r := cm.Request; r != nil {
		r.Cancel()
	}
}

type Canceler interface {
	Cancel()
}

type Context struct {
	m sync.Mutex
	cancels map[Canceler]struct{}
	reqBuf []Request
	reqId  uint32
	cancelBuf []contextMiddleware
	body []byte
}

func (c *Context) removeCanceler(cn Canceler) {
	c.m.Lock()
	delete(c.cancels, cn)
	c.m.Unlock()
}

func (c *Context) Request(msg RequestType, body []byte) (r *Request) {
	c.reqId++
	r = c.request(c.reqId, msg, body)
	if len(c.cancelBuf) == 0 {
		c.cancelBuf = make([]contextMiddleware, cxReqBuf)
	}
	m := &c.cancelBuf[0]
	c.cancelBuf = c.cancelBuf[1:]
	c.cancels[m] = struct{}{}
	r.ChainMiddleware(m)
	return
}

func (c *Context) request(id uint32, msg RequestType, body []byte) (r *Request) {
	if len(c.reqBuf) == 0 {
		c.reqBuf = make([]Request, cxReqBuf)
	}

	if len(body) > len(c.body) {
		n := cxBodyBuf
		if n < len(body) {
			n = len(body)
		}
		c.body = make([]byte, n)
	}
	copy(c.body, body)

	r = &c.reqBuf[0]
	c.reqBuf = c.reqBuf[1:]
	r.Id = id
	r.Msg = msg
	r.Body = c.body[:len(body)]
	c.body = c.body[len(body):]
	return
}
