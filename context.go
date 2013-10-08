package iproto

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
)

var _ = log.Print

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

type CxState uint32

const (
	CxCanceled = CxState(1)
	CxTimeout  = CxState(2)
)

type Context struct {
	// RetCode stores return code if case when original request were canceled or expired
	State     CxState
	parent    *Context
	m         sync.Mutex
	childCond sync.Cond
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
		c.childCond.Signal()
	}
	c.m.Unlock()
}

func (c *Context) AddCanceler(cn Canceler) {
	var ok bool
	c.m.Lock()
	c.childCond.L = &c.m
	ok = c.State == 0
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
	if atomic.CompareAndSwapUint32((*uint32)(&c.State), 0, uint32(CxCanceled)) {
		c.cancelAll()
	}
}

func (c *Context) Expire() {
	if atomic.CompareAndSwapUint32((*uint32)(&c.State), 0, uint32(CxTimeout)) {
		c.cancelAll()
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

	rc := RetCode(atomic.LoadUint32((*uint32)(&c.State)))
	if rc != 0 {
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
	rc := RetCode(atomic.LoadUint32((*uint32)(&c.State)))
	if rc != 0 {
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
	return c.State == 0
}

func (c *Context) Timeout() bool {
	return c.State == CxTimeout
}

func (c *Context) Child() (child *Context, ok bool) {
	child = &Context{parent: c}
	rc := CxState(atomic.LoadUint32((*uint32)(&c.State)))
	if rc == 0 {
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
	defer child.Done()
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
	defer child.Done()
	f(child, i)
}

func (c *Context) WaitAll() {
	c.m.Lock()
	for c.cancelsn != 0 || len(c.cancelsm) > 0 {
		c.childCond.Wait()
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

func (c *Context) Done() {
	if c.parent != nil {
		c.parent.RemoveCanceler(c)
	}
	if c.owngen {
		c.owngen = false
		c.gen.Release()
	}
}
