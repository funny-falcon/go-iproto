package iproto

import "time"

type StatService struct {
	Service
	F func(*Request, time.Duration)
}

func (ss *StatService) Send(r *Request) {
	if r.ChainBookmark(&statBookmark{f: ss.F, e: NowEpoch()}) {
		ss.Service.Send(r)
	}
}

type statBookmark struct {
	Bookmark
	f func(*Request, time.Duration)
	e Epoch
}

func (sm *statBookmark) Respond(res *Response) {
	sm.f(sm.Request, sm.e.Elapsed())
}

func StatWrap(s Service, f func(*Request, time.Duration)) Service {
	return &StatService{Service: s, F: f}
}
