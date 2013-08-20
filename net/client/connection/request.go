package connection

import (
	"github.com/funny-falcon/go-iproto"
)

type Request struct {
	iproto.BasicResponder
	fakeId uint32
}

func (r *Request) Respond(res *iproto.Response) {
	res.Id = r.Request.Id
}

