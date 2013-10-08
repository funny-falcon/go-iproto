package iproto

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	cxReqBuf    = 16
	cxReqBufMax = 128
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
	if cx := cm.cx; cx == nil {
		return
	} else if res.Code == RcCanceled {
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
	parent    *Context
	m         sync.Mutex
	child     sync.Cond
	cancels   [2]Canceler
	cancelsn  int
	cancelsm  map[Canceler]struct{}
	reqId     uint32
	owngen    bool
	gen       *RGenerator
	cancelBuf []contextMiddleware
}

func (c *Context) RemoveCanceler(cn Canceler) {
	c.m.Lock()
	var i int
	for i = 0; i < len(c.cancels); i++ {
		if cn == c.cancels[i] {
			c.cancels[i] = nil
			c.cancelsn--
			break
		}
	}
	if i == len(c.cancels) {
		delete(c.cancelsm, cn)
	}
	if c.cancelsn == 0 && len(c.cancelsm) == 0 {
		c.child.Signal()
	}
	c.m.Unlock()
}

func (c *Context) AddCanceler(cn Canceler) {
	var ok bool
	c.m.Lock()
	c.child.L = &c.m
	ok = c.RetCode != RcCanceled && c.RetCode != RcTimeout
	if ok {
		var i int
		for i = 0; i < len(c.cancels); i++ {
			if c.cancels[i] == nil {
				c.cancels[i] = cn
				c.cancelsn++
				break
			}
		}
		if i == len(c.cancels) {
			if c.cancelsm == nil {
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
		for i := 0; i < len(c.cancels); i++ {
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

func (c *Context) Respond(code RetCode, val interface{}) {
	if c.cxAsMid == nil {
		log.Panicf("Context has no binded request")
	}
	if req := c.Request; req != nil {
		w := Writer{defSize: 64}
		w.Write(val)
		req.Respond(code, w.Written())
	}
	if c.owngen {
		c.gen.Release()
	}
}

func (c *Context) NewRequest(msg RequestType, body IWriter) (r *Request, res <-chan *Response) {
	c.reqId++
	if c.gen == nil {
		c.gen = GetGenerator()
		c.owngen = true
	}
	r = c.gen.Request(c.reqId, msg, body)
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
		m.c = c
		c.cancelBuf = c.cancelBuf[1:]
		r.ChainMiddleware(m)
		c.AddCanceler(m)
	}
	return
}

func (c *Context) NewMulti() (multi *MultiRequest) {
	if c.gen == nil {
		c.gen = GetGenerator()
		c.owngen = true
	}
	multi = &MultiRequest{cx: c, gen: c.gen}
	rc := RetCode(atomic.LoadUint32((*uint32)(&c.RetCode)))
	if rc == RcCanceled || rc == RcTimeout {
		multi.Cancel()
	} else {
		c.AddCanceler(multi)
	}
	return multi
}

func (c *Context) SendMsgBody(serv Service, msg RequestType, body interface{}) (req *Request, res <-chan *Response) {
	wr := Wrap2IWriter(body)
	req, res = c.NewRequest(msg, wr)
	serv.Send(req)
	return
}

func (c *Context) CallMsgBody(serv Service, msg RequestType, body interface{}) *Response {
	var req *Request
	wr := Wrap2IWriter(body)
	req, res := c.NewRequest(msg, wr)
	serv.Send(req)
	return <-res
}

func (c *Context) Send(serv Service, r RequestData) (req *Request, res <-chan *Response) {
	req, res = c.NewRequest(r.IMsg(), r)
	serv.Send(req)
	return req, res
}

func (c *Context) Call(serv Service, r RequestData) *Response {
	req, res := c.NewRequest(r.IMsg(), r)
	serv.Send(req)
	return <-res
}

func (c *Context) Alive() bool {
	return c.RetCode == 0
}

func (c *Context) Timeout() bool {
	return c.RetCode == RcTimeout
}

func (c *Context) Child() (child *Context, ok bool) {
	child = &Context{parent: c}
	rc := RetCode(atomic.LoadUint32((*uint32)(&c.RetCode)))
	if ok = !(rc == RcCanceled || rc == RcTimeout); ok {
		c.AddCanceler(child)
	} else {
		child.Cancel()
	}
	return
}

func (c *Context) Go(f func(cx *Context)) (child *Context) {
	var ok bool
	if child, ok = c.Child(); ok {
		go child.go_(f)
	}
	return
}

func (child *Context) go_(f func(cx *Context)) {
	defer child.parent.RemoveCanceler(child)
	f(child)
}

func (c *Context) GoInt(f func(*Context, interface{}), i interface{}) (child *Context) {
	var ok bool
	if child, ok = c.Child(); ok {
		go child.goInt(f, i)
	}
	return
}

func (child *Context) goInt(f func(*Context, interface{}), i interface{}) {
	defer child.parent.RemoveCanceler(child)
	f(child, i)
}

func (c *Context) WaitAll() {
	c.m.Lock()
	for c.cancelsn != 0 || len(c.cancelsm) > 0 {
		c.child.Wait()
	}
	c.m.Unlock()
}

func (c *Context) GoAsync(f func(cx *Context)) (child *Context) {
	var ok bool
	if child, ok = c.Child(); ok {
		go f(child)
	}
	return
}

func (c *Context) GoIntAsync(f func(*Context, interface{}), i interface{}) (child *Context) {
	var ok bool
	if child, ok = c.Child(); ok {
		go f(child, i)
	}
	return
}

func (child *Context) Done() {
	child.parent.RemoveCanceler(child)
	if child.owngen {
		child.gen.Release()
	}
}
