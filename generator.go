package iproto

import (
	"github.com/funny-falcon/go-iproto/marshal"
	"sync"
)

const rrsize = 32

type generators chan *RGenerator
type RGenerator struct {
	req *[rrsize]Request
	res *[rrsize]Response
	w   marshal.Writer
	g   generators
	m   sync.Mutex
	i   int32
}

func (gen *RGenerator) Request(id uint32, msg RequestType, val interface{}) (req *Request) {
	var res *Response
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
	req.Id = id
	req.Msg = msg
	var ok bool
	if req.Body, ok = val.(Body); !ok {
		gen.w.Write(val)
		req.Body = gen.w.Written()
	}
	return
}

func (gen *RGenerator) Release() {
	gen.m.Lock()
	if gen != nil && gen.g != nil {
		g := gen.g
		gen.g = nil
		select {
		case g <- gen:
		default:
		}
	}
	gen.m.Unlock()
}

func (g generators) Get() (gen *RGenerator) {
	select {
	case gen = <-g:
	default:
		gen = &RGenerator{g: g}
	}
	return
}

var gencache = make(generators, 128)

func GetGenerator() *RGenerator {
	return gencache.Get()
}
