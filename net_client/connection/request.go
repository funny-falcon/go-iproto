package connection

import (
	"github.com/funny-falcon/go-iproto"
)

type Request struct {
	iproto.BasicResponder
	conn *Connection
	fakeId uint32
}

func wrapRequest(conn *Connection, ireq *iproto.Request, id uint32) *Request {
	req := &Request {
		conn: conn,
		fakeId: id,
	}
	req.Chain(ireq)
	return req
}

func (r *Request) Respond(res iproto.Response) {
	prev := r.Unchain()
	if prev != nil {
		res.Id = r.Request.Id
		prev.Respond(res)
	}
}

func (r *Request) Cancel() {
	prev := r.Unchain()
	if prev != nil {
		prev.Cancel()
	}
}
