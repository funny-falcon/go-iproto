package iproto

import (
	"sync"
	"sync/atomic"
	"log"
	"runtime"
)

const (
	cxReqBuf = 32
	cxBodyBuf = 256
)

type contextMiddleware struct {
	Middleware
	c *Context
}

func (cm *contextMiddleware) Respond(res *Response) {
	if c := cm.c; c != nil {
		c.RemoveCanceler(cm)
	}
}

func (cm *contextMiddleware) Cancel() {
	if r := cm.Request; r != nil {
		r.Cancel()
	}
}

type Canceler interface {
	Cancel()
}

type cxAsMid struct {
	Middleware
	cx *Context
}

func (cm *cxAsMid) Respond(res *Response) {
	cx := cm.cx
	if res.Code == RcCanceled {
		cx.Cancel()
	} else if res.Code == RcTimeout {
		cx.Expire()
		cm.Request.ResetToPending()
		cm.Request.SetInFly(nil)
	}
}

type Context struct {
	// RetCode stores return code if case when original request were canceled or expired
	*cxAsMid
	RetCode
	child sync.WaitGroup
	parent *sync.WaitGroup
	m sync.Mutex
	cancels [2]Canceler
	cancelsn int
	cancelsm map[Canceler]struct{}
	reqBuf []Request
	resBuf []Response
	reqId  uint32
	cancelBuf []contextMiddleware
	body []byte
}

func (c *Context) RemoveCanceler(cn Canceler) {
	c.m.Lock()
	var i int
	for i=0; i<len(c.cancels); i++ {
		if cn == c.cancels[i] {
			c.cancels[i] = nil
			c.cancelsn--
			break
		}
	}
	if i == len(c.cancels) {
		delete(c.cancelsm, cn)
	}
	c.m.Unlock()
}

func (c *Context) AddCanceler(cn Canceler) {
	var ok bool
	c.m.Lock()
	ok = c.RetCode != RcCanceled && c.RetCode != RcTimeout
	if ok {
		var i int
		for i=0; i<len(c.cancels); i++ {
			if c.cancels[i] == nil {
				c.cancels[i] = cn
				c.cancelsn++
				break
			}
		}
		if i == len(c.cancels) {
			if c.cancelsm == nil  {
				c.cancelsm = make(map[Canceler]struct{})
			}
			c.cancelsm[cn] = struct{}{}
		}
	}
	c.m.Unlock()
	if !ok {
		cn.Cancel()
	}
}

func (c *Context) cancelAll() {
	for len(c.cancelsm) > 0 || c.cancelsn > 0 {
		c.m.Lock()
		cancels := make([]Canceler, 0, len(c.cancels)+c.cancelsn)
		for i:=0; i<len(c.cancels); i++ {
			if c.cancels[i] != nil {
				cancels = append(cancels, c.cancels[i])
			}
		}
		for cancel := range c.cancelsm {
			cancels = append(cancels, cancel)
		}
		c.m.Unlock()
		for _, cancel := range cancels {
			cancel.Cancel()
		}
		runtime.Gosched()
	}
}

func (c *Context) Cancel() {
	if atomic.CompareAndSwapUint32((*uint32)(&c.RetCode), 0, uint32(RcCanceled)) {
		c.cancelAll()
	}
}

func (c *Context) Expire() {
	if atomic.CompareAndSwapUint32((*uint32)(&c.RetCode), 0, uint32(RcTimeout)) {
		c.cancelAll()
	}
}

func (c *Context) Respond(code RetCode, body []byte) {
	if c.cxAsMid == nil {
		log.Panicf("Context has no binded request")
	}
	if req := c.Request; req != nil {
		req.Respond(code, body)
	}
}

func (c *Context) NewRequest(msg RequestType, body []byte) (r *Request, res <-chan *Response) {
	c.reqId++
	r = c.request(c.reqId, msg, body)
	ch := make(Chan, 1)
	res, r.Responder = ch, ch

	rc := RetCode(atomic.LoadUint32((*uint32)(&c.RetCode)))
	if rc == RcCanceled || rc == RcTimeout {
		r.Cancel()
	} else {
		if len(c.cancelBuf) == 0 {
			c.cancelBuf = make([]contextMiddleware, cxReqBuf)
		}
		m := &c.cancelBuf[0]
		c.cancelBuf = c.cancelBuf[1:]
		r.ChainMiddleware(m)
		c.AddCanceler(m)
	}
	return
}

func (c *Context) request(id uint32, msg RequestType, body []byte) (r *Request) {
	if len(c.reqBuf) == 0 {
		c.reqBuf = make([]Request, cxReqBuf)
		c.resBuf = make([]Response, cxReqBuf)
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
	r.Response = &c.resBuf[0]
	c.resBuf = c.resBuf[1:]

	r.Body = c.body[:len(body)]
	c.body = c.body[len(body):]
	return
}

func (c *Context) NewMulti() (multi *MultiRequest) {
	multi = &MultiRequest{cx: c}
	rc := RetCode(atomic.LoadUint32((*uint32)(&c.RetCode)))
	if rc == RcCanceled || rc == RcTimeout {
		multi.Cancel()
	} else {
		c.AddCanceler(multi)
	}
	return multi
}

func (c *Context) Alive() bool {
	return c.RetCode == 0
}

func (c *Context) Timeout() bool {
	return c.RetCode == RcTimeout
}

func (c *Context) Go(f func(cx *Context)) {
	child := &Context{parent: &c.child}
	c.AddCanceler(child)
	c.child.Add(1)
	go c.go_(child, f)
}

func (c *Context) go_(child *Context, f func(cx *Context)) {
	defer c.RemoveCanceler(child)
	f(child)
}

func (c *Context) GoInt(f func(*Context, int), i int) {
	child := &Context{parent: &c.child}
	c.AddCanceler(child)
	c.child.Add(1)
	go c.goInt(child, f, i)
}

func (c *Context) goInt(child *Context, f func(*Context, int), i int) {
	defer c.RemoveCanceler(child)
	f(child, i)
}

func (c *Context) GoRest(f func(*Context, ...interface{}), rest... interface{}) {
	child := &Context{parent: &c.child}
	c.AddCanceler(child)
	c.child.Add(1)
	go c.goRest(child, f, rest)
}

func (c *Context) goRest(child *Context, f func(*Context, ...interface{}), rest []interface{}) {
	defer c.RemoveCanceler(child)
	f(child, rest...)
}

func (c *Context) Wait() {
	c.child.Wait()
}

func (c *Context) Done() {
	c.parent.Done()
}
