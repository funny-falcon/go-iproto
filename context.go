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
	cancelBuf []contextMiddleware
	writer    Writer
	gen       *RRGenerator
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
		c.writer = Writer{}
		w := Writer{defSize: 64}
		w.Write(val)
		req.Respond(code, w.Written())
	}
	PutGenerator(c.gen)
}

func (c *Context) request(id uint32, msg RequestType, val IWriter) (r *Request) {
	if c.gen == nil {
		c.gen = GetGenerator()
	}

	var body []byte
	var ok bool
	if body, ok = val.(Body); !ok {
		val.IWrite(val, &c.writer)
		body = c.writer.Written()
	}

	r, _ = c.gen.Pair()
	r.Id = id
	r.Msg = msg
	r.Body = body
	return
}

func (c *Context) NewRequest(msg RequestType, body IWriter) (r *Request, res <-chan *Response) {
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
		m.c = c
		c.cancelBuf = c.cancelBuf[1:]
		r.ChainMiddleware(m)
		c.AddCanceler(m)
	}
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
	PutGenerator(child.gen)
}

const rrsize = 32

type RRGenerator struct {
	req *[rrsize]Request
	res *[rrsize]Response
	i   int32
}

func (gen *RRGenerator) Pair() (req *Request, res *Response) {
	if gen.req == nil {
		gen.req = &[rrsize]Request{}
		gen.res = &[rrsize]Response{}
	}
	req = &gen.req[gen.i]
	res = &gen.res[gen.i]
	req.Response = res
	if gen.i++; gen.i == rrsize {
		gen.i = 0
		gen.req = nil
		gen.res = nil
	}
	return
}

var gencache = make(chan *RRGenerator, 1024)

func GetGenerator() (gen *RRGenerator) {
	select {
	case gen = <-gencache:
	default:
		gen = &RRGenerator{}
	}
	return
}

func PutGenerator(gen *RRGenerator) {
	if gen != nil {
		select {
		case gencache <- gen:
		default:
		}
	}
}
