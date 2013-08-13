package iproto

type EndPoint interface {
	IProtoStop()
	IProtoRun(chan *Request)
	RequestChan() chan<- *Request
	DefaultDeadline() Deadline
	DefaultTimeout() Timeout
}

type SimplePoint chan *Request

func (s SimplePoint) RequestChan() chan<- *Request {
	return (chan<- *Request)(s)
}

func (s SimplePoint) DefaultDeadline() (d Deadline) {
	return
}

func (s SimplePoint) DefaultTimeout() (d Timeout) {
	return
}

func sendRequest(serv EndPoint, req *Request) {
	serv.RequestChan() <- req
}

func Send(serv EndPoint, request Request) Canceler {
	deadline := request.Deadline
	if deadline.Zero() {
		deadline = serv.DefaultDeadline()
	}
	req := &Request{
		Id:       request.Id,
		Msg:      request.Msg,
		Body:     request.Body,
		Callback: request.Callback,
		Deadline: deadline,
	}
	req.SetPending()

	serv.RequestChan() <- req
	return req
}

func SendDeadline(serv EndPoint, request Request, deadline Deadline) Canceler {
	if deadline.Zero() {
		deadline = serv.DefaultDeadline()
	}
	req := &Request{
		Id:       request.Id,
		Msg:      request.Msg,
		Body:     request.Body,
		Callback: request.Callback,
		Deadline: deadline,
	}
	req.SetPending()
	serv.RequestChan() <- req
	return req
}

func SendTimeout(serv EndPoint, request Request, timeout Timeout) Canceler {
	if timeout.Zero() {
		timeout = serv.DefaultTimeout()
	}
	req := &Request{
		Id:       request.Id,
		Msg:      request.Msg,
		Body:     request.Body,
		Callback: request.Callback,
		Deadline: timeout.NowDeadline(),
	}
	req.SetPending()
	serv.RequestChan() <- req
	return req
}
