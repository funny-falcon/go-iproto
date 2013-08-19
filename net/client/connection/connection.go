package connection

import (
	"bufio"
	"github.com/funny-falcon/go-iproto"
	nt "github.com/funny-falcon/go-iproto/net"
	"io"
	"log"
	"net"
	"time"
	"sync"
)

type notifyAction uint32

const (
	writeClosed = notifyAction(iota + 1)
	readClosed
)

type ErrorWhen uint8

const (
	Dial = ErrorWhen(iota + 1)
	Read
	Write
)

type Error struct {
	Conn  *Connection
	When  ErrorWhen
	Error error
}

type ConnState uint32
const (
	CsNew = 1 << iota
	CsDialing
	CsConnected
	CsReadClosed
	CsWriteClosed
	CsClosed = CsReadClosed | CsWriteClosed
)

type CConf struct {
	Network string
	Address string

	PingInterval time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	DialTimeout  time.Duration

	RetCodeLen int

	ConnErr chan<- Error
}

type Connection struct {
	iproto.SimplePoint
	Id uint64
	sync.Mutex
	*CConf

	conn NetConn

	closeWrite   chan bool
	readErr      error
	controlOk    bool

	inFly        RequestHolder

	State        ConnState
	shutdown     bool

	readTimeout  nt.Timeout
	writeTimeout nt.Timeout

	loopNotify chan notifyAction
}
var _ iproto.EndPoint = (*Connection)(nil)

func NewConnection(conf *CConf, id uint64) (conn *Connection) {
	conn = &Connection{
		CConf: conf,
		Id:    id,

		controlOk:    true,

		inFly:        RequestHolder{reqs: make(reqMap)},

		readTimeout:  nt.Timeout{Timeout: conf.ReadTimeout, Kind: nt.Read},
		writeTimeout: nt.Timeout{Timeout: conf.WriteTimeout, Kind: nt.Write},

		loopNotify: make(chan notifyAction, 2),
	}
	conn.SimplePoint.Init()
	return
}

/* default 5 seconds interval for Connection */
const DialTimeout = 5 * time.Second

func (conn *Connection) Run(ch chan *iproto.Request) {
	conn.SetChan(ch)
	go conn.dial()
}

func (conn *Connection) dial() {
	dialer := net.Dialer{Timeout: DialTimeout}
	if netconn, err := dialer.Dial(conn.Network, conn.Address); err != nil {
		conn.ConnErr <- Error{conn, Dial, err}
		conn.State = CsClosed
	} else {
		conn.conn = netconn.(NetConn)
		conn.ConnErr <- Error{conn, Dial, nil}
		conn.State = CsConnected
		go conn.readLoop()
		go conn.writeLoop()
		go conn.controlLoop()
	}
}

/* RunWithConn is for testing purposes */
func (conn *Connection) RunWithConn(netconn io.ReadWriteCloser) {
	switch nc := netconn.(type) {
	case NetConn:
		conn.conn = nc
	default:
		conn.conn = rwcWrapper{ReadWriteCloser: netconn}
	}
	conn.ConnErr <- Error{conn, Dial, nil}
	go conn.readLoop()
	go conn.writeLoop()
	conn.controlLoop()
}

func (conn *Connection) SetReadTimeout(t time.Duration) {
	if conn.State & CsReadClosed == 0 {
		conn.readTimeout.Set(conn.conn, t)
	}
}

func (conn *Connection) SetWriteTimeout(t time.Duration) {
	if conn.State & CsWriteClosed == 0 {
		conn.readTimeout.Set(conn.conn, t)
	}
}

func (conn *Connection) controlLoopExit() {
	if conn.State & CsWriteClosed == 0 {
		conn.Stop()
	}
	conn.ConnErr <- Error{conn, Read, conn.readErr}
	conn.flushInFly()
}

func (conn *Connection) controlLoop() {
	var closeReadCalled bool
	defer conn.controlLoopExit()
	for {
		action := <-conn.loopNotify
		switch action {
		case writeClosed:
			conn.State |= CsWriteClosed
		case readClosed:
			conn.State |= CsReadClosed
			if conn.State & CsWriteClosed == 0 {
				conn.Stop()
			}
		}

		if conn.State & CsWriteClosed == 0 {
			if !closeReadCalled && conn.inFly.count.Get() == 0 {
				conn.conn.CloseRead()
				closeReadCalled = true
			}
			if conn.State & CsReadClosed == 0 {
				break
			}
		}
	}
}

func (conn *Connection) putInFly(request *iproto.Request) *Request {
	req := conn.inFly.getNext(conn)
	if request.SetInFly(req) {
		return req
	}
	conn.inFly.putBack(req)
	return nil
}

func (conn *Connection) flushInFly() {
	reqs := conn.inFly.getAll()
	conn.inFly = RequestHolder{}

	resp := iproto.Response{Code: iproto.RcIOError}
	if conn.shutdown {
		resp.Code = iproto.RcShutdown
	}
	for _, req := range reqs {
		resp.Msg = req.Request.Msg
		resp.Id = req.fakeId
		req.Request.Response(resp, req)
	}
}

func (conn *Connection) notifyLoop(action notifyAction) {
	conn.loopNotify <- action
}

func (conn *Connection) readLoop() {
	var res nt.Response
	var header nt.HeaderIO
	header.Init()

	defer conn.notifyLoop(readClosed)
	defer conn.readTimeout.Freeze(nil)

	read := bufio.NewReaderSize(conn.conn, 64*1024)
	conn.readTimeout.UnFreeze(conn.conn)

	for {
		conn.readTimeout.Reset(conn.conn)

		if res, conn.readErr = header.ReadResponse(read, conn.RetCodeLen); conn.readErr != nil {
			break
		}

		if res.Id == iproto.PingRequestId && res.Msg == iproto.Ping {
			continue
		}

		req := conn.inFly.get(res.Id)
		if req == nil {
			log.Panicf("No mathing request: %v %v", res.Msg, res.Id)
		}

		if ireq := req.Request; ireq != nil {
			ireq.Response(iproto.Response(res), req)
		}

		conn.inFly.putBack(req)
	}
}

const fakePingInterval = 1 * time.Hour

func (conn *Connection) writeLoop() {
	var err error
	var header nt.HeaderIO
	var pingTicker *time.Ticker


	write := bufio.NewWriterSize(conn.conn, 16*1024)

	if conn.PingInterval > 0 {
		pingTicker = time.NewTicker(conn.PingInterval)
	} else {
		pingTicker = time.NewTicker(fakePingInterval)
		pingTicker.Stop()
	}

	defer func() {
		conn.writeTimeout.Freeze(nil)
		pingTicker.Stop()
		if err = write.Flush(); err == nil {
			conn.conn.CloseWrite()
		}
		conn.notifyLoop(writeClosed)
		conn.ConnErr <- Error{conn, Write, err}
	}()

	header.Init()

	conn.writeTimeout.UnFreeze(conn.conn)
Loop:
	for {
		var request *iproto.Request
		var req *Request
		var okRequest, ping bool
		var requestHeader nt.Request

		conn.writeTimeout.Reset(conn.conn)

		select {
		case <-conn.ExitChan():
		case request, okRequest = <-conn.ReceiveChan():
		default:
			if err = write.Flush(); err != nil {
				break Loop
			}
			conn.writeTimeout.Freeze(conn.conn)
			select {
			case <-pingTicker.C:
				ping, okRequest = true, true
			case request, okRequest = <-conn.ReceiveChan():
			case <-conn.ExitChan():
			}
			conn.writeTimeout.UnFreeze(conn.conn)
		}

		if !okRequest {
			break
		}

		if ping {
			requestHeader = nt.Request{
				Msg:  iproto.Ping,
				Body: make([]byte, 0),
				Id:   iproto.PingRequestId,
			}
		} else {
			if request.SendExpired() {
				continue
			}
			if req = conn.putInFly(request); req == nil {
				continue
			}
			requestHeader = nt.Request{
				Msg: request.Msg,
				Id: req.fakeId,
				Body: request.Body,
			}
		}

		if err = header.WriteRequest(write, requestHeader); err != nil {
			break
		}
	}
}

func (conn *Connection) Closed() bool {
	return conn.State & CsClosed != 0
}

func (conn *Connection) LocalAddr() net.Addr {
	return conn.conn.LocalAddr()
}

func (conn *Connection) RemoteAddr() net.Addr {
	return conn.conn.RemoteAddr()
}
