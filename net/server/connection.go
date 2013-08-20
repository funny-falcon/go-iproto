package server

import (
	"github.com/funny-falcon/go-iproto"
	nt "github.com/funny-falcon/go-iproto/net"
	"log"
	"sync"
	"bufio"
)
var _ = log.Print

type notifyAction uint32

const (
	writeClosed = notifyAction(iota + 1)
	readClosed
	inFlyEmpty
)

type ConnState uint32
const (
	CsConnected = ConnState(1 << iota)
	CsReadClosed
	CsWriteClosed
	CsClosed = CsReadClosed | CsWriteClosed
)

type Connection struct {
	*Server
	Id uint64
	conn nt.NetConn

	buffer ResponseBuffer

	state ConnState

	inFly  map[uint32] *iproto.Request
	sync.Mutex

	readTimeout  nt.Timeout
	writeTimeout nt.Timeout

	loopNotify chan notifyAction
}

func NewConnection(serv *Server, connection nt.NetConn, id uint64) (conn *Connection) {
	log.Print("New connection ", id, connection.RemoteAddr())
	conn = &Connection {
		Server: serv,
		Id: id,
		conn: connection,

		inFly: make(map[uint32] *iproto.Request),

		readTimeout:  nt.Timeout{Timeout: serv.ReadTimeout, Kind: nt.Read},
		writeTimeout: nt.Timeout{Timeout: serv.WriteTimeout, Kind: nt.Write},

		state: CsConnected,

		loopNotify: make(chan notifyAction, 2),
	}
	return
}

func (conn *Connection) Run() {
	conn.buffer.Init()
	go conn.controlLoop()
	go conn.readLoop()
	go conn.writeLoop()
}

func (conn *Connection) Stop() {
	conn.conn.CloseRead()
}

func (conn *Connection) Respond(r iproto.Response) {
	var ok bool
	conn.Lock()
	if _, ok = conn.inFly[r.Id]; ok {
		delete(conn.inFly, r.Id)
	}
	if conn.state & CsReadClosed != 0 && len(conn.inFly) == 0 {
		conn.notifyLoop(inFlyEmpty)
	}
	conn.Unlock()
	if ok {
		conn.buffer.in <- nt.Response(r)
	}
}

func (conn *Connection) closed() {
	log.Print("Closed ", conn.Id, conn.conn.RemoteAddr())
	conn.Lock()
	reqs := make([]*iproto.Request, 0, len(conn.inFly))
	for _, req := range conn.inFly {
		reqs[len(reqs)] = req
	}
	conn.Unlock()
	for _, req := range reqs {
		req.Cancel()
	}
	conn.Server.connClosed <- conn.Id
}

func (conn *Connection) controlLoop() {
	defer conn.closed()
	inIsClosed := false
	for {
		action := <-conn.loopNotify
		switch action {
		case readClosed:
			conn.state &= CsClosed
			conn.state |= CsReadClosed
			log.Print("Read Closed ", conn.Id, conn.state, conn.conn.RemoteAddr())
		case writeClosed:
			conn.state &= CsClosed
			conn.state |= CsWriteClosed
			log.Print("Write Closed ", conn.Id, conn.state, conn.conn.RemoteAddr())
			if conn.state & CsReadClosed == 0 {
				conn.conn.CloseRead()
			}
		case inFlyEmpty:
			log.Print("InFly Empty", conn.Id, conn.state, conn.conn.RemoteAddr())
		}

		if !inIsClosed && conn.state & CsReadClosed != 0 && len(conn.inFly) == 0 {
			inIsClosed = true
			conn.buffer.CloseIn()
		}

		if conn.state & CsClosed == CsClosed {
			break
		}
	}
}

func (conn *Connection) notifyLoop(action notifyAction) {
	conn.loopNotify <- action
}

func (conn *Connection) readLoop() {
	var req nt.Request
	var err error
	var header nt.HeaderIO
	header.Init()

	defer conn.notifyLoop(readClosed)
	defer conn.readTimeout.Freeze(nil)

	read := bufio.NewReaderSize(conn.conn, 64*1024)
	conn.readTimeout.UnFreeze(conn.conn)

	for {
		conn.readTimeout.Reset(conn.conn)

		if req, err = header.ReadRequest(read); err != nil {
			break
		}

		if req.Msg == iproto.Ping {
			res := nt.Response{
				Id: req.Id,
				Msg: iproto.Ping,
			}
			conn.buffer.out <- res
			continue
		}

		request := iproto.Request{
			Id: req.Id,
			Msg: req.Msg,
			Body: req.Body,
			Responder: conn,
		}
		request.SetPending()
		conn.Lock()
		conn.inFly[request.Id] = &request
		conn.Unlock()

		conn.safeSend(&request)
	}
}

func (conn *Connection) sendRescue(req *iproto.Request) {
	if err := recover(); err != nil {
		res := iproto.Response{
			Id: req.Id,
			Msg: req.Msg,
			Code: iproto.RcFailed,
		}
		req.Response(res, nil)
	}
}

func (conn *Connection) safeSend(req *iproto.Request) {
	defer conn.sendRescue(req)
	conn.EndPoint.Send(req)
}

func (conn *Connection) writeLoop() {
	var err error
	var header nt.HeaderIO

	write := bufio.NewWriterSize(conn.conn, 16*1024)

	defer func() {
		conn.writeTimeout.Freeze(nil)
		if err == nil {
			if err = write.Flush(); err == nil {
				conn.conn.CloseWrite()
			}
		}
		conn.notifyLoop(writeClosed)
	}()


	header.Init()

	conn.writeTimeout.UnFreeze(conn.conn)
Loop:
	for {
		var res nt.Response
		var ok bool

		select {
		case res, ok = <-conn.buffer.out:
			if !ok {
				break Loop
			}
		default:
			if err = write.Flush(); err != nil {
				break Loop
			}
			conn.writeTimeout.Freeze(conn.conn)
			if res, ok = <-conn.buffer.out; !ok {
				break Loop
			}
		}

		if err = header.WriteResponse(conn.conn, res, conn.RetCodeLen); err != nil {
			break Loop
		}
	}
}
