package connection

import (
	"bufio"
	"github.com/funny-falcon/go-iproto"
	nt "github.com/funny-falcon/go-iproto/net_timeout"
	"io"
	"log"
	"net"
	"time"
	"sync"
)

type ConnControlKind int

const (
	CloseWrite = ConnControlKind(iota + 1)
	ReadTimeout
	WriteTimeout
	PingInterval
)

type Control struct {
	Kind     ConnControlKind
	Duration time.Duration
}

type notifyAction uint32

const (
	addInFly = notifyAction(iota + 1)
	decInFly
	writeClosed
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

type CConf struct {
	Network string
	Address string

	PingInterval time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	RetCodeLen int

	ConnErr chan<- Error
}

type Connection struct {
	Id uint32
	*CConf
	conn NetConn

	Control      chan Control
	writeControl chan Control
	readErr      error
	orphaned     bool

	requests chan *iproto.Request
	inFly    map[uint32]*iproto.Request
	sync.Mutex

	Dialing    bool
	closing    bool
	readClosed bool

	readTimeout  nt.Timeout
	writeTimeout nt.Timeout

	loopNotify chan notifyAction
}

func NewConnection(conf *CConf, id uint32) (conn *Connection) {
	conn = &Connection{
		CConf: conf,
		Id:    id,

		Control:      make(chan Control, 1),
		writeControl: make(chan Control, 2),
		orphaned:     false,

		inFly:        make(map[uint32]*iproto.Request),
		readTimeout:  nt.Timeout{Timeout: conf.ReadTimeout},
		writeTimeout: nt.Timeout{Timeout: conf.WriteTimeout},

		loopNotify: make(chan notifyAction, 2),

		Dialing:    true,
	}
	conn.readTimeout.Init()
	conn.writeTimeout.Init()
	return
}

func (conn *Connection) Established() bool {
	return !conn.Dialing && !(conn.closing || conn.readClosed)
}


/* default 5 seconds interval for Connection */
const DialTimeout = 5 * time.Second

func (conn *Connection) IProtoStop() {
	conn.Control <- Control{Kind: CloseWrite}
}

func (conn *Connection) IProtoRun(requests chan *iproto.Request) {
	conn.requests = requests
	go conn.dial()
}

func (conn *Connection) dial() {
	dialer := net.Dialer{Timeout: DialTimeout}
	if netconn, err := dialer.Dial(conn.Network, conn.Address); err != nil {
		conn.ConnErr <- Error{conn, Dial, err}
		conn.Dialing = false
		conn.closing = true
		conn.readClosed = true
	} else {
		conn.conn = netconn.(NetConn)
		conn.ConnErr <- Error{conn, Dial, nil}
		go conn.readLoop()
		go conn.writeLoop()
		go conn.controlLoop()
	}
}

/* RunWithConn is for testing purposes */
func (conn *Connection) RunWithConn(requests chan *iproto.Request, netconn NetConn) {
	conn.requests = requests
	conn.conn = netconn
	conn.ConnErr <- Error{conn, Dial, nil}
	go conn.readLoop()
	go conn.writeLoop()
	conn.controlLoop()
}

/* RunWithReadWriteCloser is for testing purposes */
func (conn *Connection) RunWithReadWriteCloser(requests chan *iproto.Request, netconn io.ReadWriteCloser) {
	conn.requests = requests
	conn.conn = rwcWrapper{ReadWriteCloser: netconn}
	conn.ConnErr <- Error{conn, Dial, nil}
	go conn.readLoop()
	go conn.writeLoop()
	conn.controlLoop()
}

func (conn *Connection) putInFly(req *iproto.Request) {
	conn.Lock()
	defer conn.Unlock()
	if ex, ok := conn.inFly[req.Id]; ok {
		log.Panicf("Duplicate requests %+v %+v", ex, req)
	}
	conn.inFly[req.Id] = req
}

func (conn *Connection) respondInFly(res iproto.Response) {
	conn.Lock()
	defer conn.Unlock()
	req := conn.inFly[res.Id]
	delete(conn.inFly, res.Id)
	if req == nil {
		log.Panicf("No mathing request: %v %v", res.Msg, res.Id)
	}
	req.Response(res)
}

func (conn *Connection) flushInFly() {
	var req *iproto.Request
	conn.Lock()
	defer conn.Unlock()
	resp := iproto.Response{Code: iproto.RcIOError}
	for resp.Id, req = range conn.inFly {
		resp.Msg = req.Msg
		req.Response(resp)
	}
}

func (conn *Connection) checkControl() {
	if conn.orphaned {
		return
	}
	select {
	case control, ok := <-conn.Control:
		if !ok {
			/* do not know what to do :( we are orphaned */
			/* hope, reader and writer will signal us */
			conn.orphaned = true
			conn.conn.Close()
		} else {
			switch control.Kind {
			case ReadTimeout:
				if !conn.readClosed {
					conn.readTimeout.Timeout = control.Duration
					conn.readTimeout.DoAction(conn.conn, nt.Read, nt.Reset)
				}
			case WriteTimeout:
				conn.writeTimeout.Timeout = control.Duration
				conn.writeTimeout.DoAction(conn.conn, nt.Write, nt.Reset)
			case CloseWrite, PingInterval:
				conn.writeControl <- control
			default:
				log.Panicf("Unknown Connection control kind %d", control.Kind)
			}
		}
	default:
	}
}

func (conn *Connection) controlLoopExit() {
	if !conn.closing {
		conn.writeControl <- Control{Kind: CloseWrite}
	}
	conn.ConnErr <- Error{conn, Read, conn.readErr}
	for len(conn.inFly) != 0 {
		conn.flushInFly()
	}
}

func (conn *Connection) controlLoop() {
	var closeReadCalled bool
	defer conn.controlLoopExit()
	for {
		conn.checkControl()
		select {
		case action := <-conn.readTimeout.Actions:
			conn.readTimeout.DoAction(conn.conn, nt.Read, action)
		case action := <-conn.writeTimeout.Actions:
			conn.readTimeout.DoAction(conn.conn, nt.Write, action)
		case action := <-conn.loopNotify:
			switch action {
			case writeClosed:
				conn.closing = true
			case readClosed:
				conn.readClosed = true
				if !conn.closing {
					conn.writeControl <- Control{Kind: CloseWrite}
				}
			}
		}
		if conn.closing {
			if !closeReadCalled && len(conn.inFly) == 0 {
				conn.conn.CloseRead()
				closeReadCalled = true
			}
			if conn.readClosed {
				break
			}
		}
	}
}

func (conn *Connection) notifyLoop(action notifyAction) {
	conn.loopNotify <- action
}

func (conn *Connection) readLoop() {
	var res iproto.Response
	var header iproto.Header
	header.Init()

	defer conn.notifyLoop(readClosed)

	read := bufio.NewReaderSize(conn.conn, 64*1024)
	conn.readTimeout.PingAction(nt.UnFreeze)

	for {
		conn.readTimeout.PingAction(nt.Reset)

		if res, conn.readErr = header.ReadResponse(read, conn.RetCodeLen); conn.readErr != nil {
			break
		}

		if res.Id == iproto.PingRequestId && res.Msg == iproto.Ping {
			continue
		}

		conn.respondInFly(res)
	}
}

const fakePingInterval = 1 * time.Hour

func (conn *Connection) writeLoop() {
	var err error
	var header iproto.Header
	var pingTicker *time.Ticker


	write := bufio.NewWriterSize(conn.conn, 64*1024)

	defer func() {
		err = write.Flush()
		if err == nil {
			conn.conn.CloseWrite()
		}
		conn.notifyLoop(writeClosed)
		conn.ConnErr <- Error{conn, Write, err}
	}()

	header.Init()

	pingRequest := iproto.Request{
		Msg:  iproto.Ping,
		Body: make([]byte, 0),
		Id:   iproto.PingRequestId,
	}

	if conn.PingInterval > 0 {
		pingTicker = time.NewTicker(conn.PingInterval)
	} else {
		pingTicker = time.NewTicker(fakePingInterval)
		pingTicker.Stop()
	}
	defer func() { pingTicker.Stop() }()

	conn.writeTimeout.PingAction(nt.UnFreeze)
Loop:
	for {
		var request *iproto.Request
		var control Control
		var okRequest, okControl bool

		conn.writeTimeout.PingAction(nt.Reset)

		select {
		case control, okControl = <-conn.writeControl:
		default:
			select {
			case <-pingTicker.C:
				request, okRequest = &pingRequest, true
			case request, okRequest = <-conn.requests:
			default:
				write.Flush()
				conn.writeTimeout.PingAction(nt.Freeze)
				select {
				case <-pingTicker.C:
					request, okRequest = &pingRequest, true
				case request, okRequest = <-conn.requests:
				case control, okControl = <-conn.writeControl:
				}
				conn.writeTimeout.PingAction(nt.UnFreeze)
			}
		}

		if okControl {
			switch control.Kind {
			case CloseWrite:
				break Loop
			case PingInterval:
				pingTicker.Stop()
				if control.Duration > 0 {
					pingTicker = time.NewTicker(control.Duration)
				} else {
					pingTicker = time.NewTicker(fakePingInterval)
					pingTicker.Stop()
				}
				continue Loop
			default:
				log.Panicf("Write loop do not understand control.kind %d", control.Kind)
			}
		}

		if !okRequest {
			break
		}

		if request.Id != iproto.PingRequestId {
			if !request.SetInFly() {
				continue
			}
			conn.putInFly(request)
		}

		if err = header.WriteRequest(write, request); err != nil {
			break
		}

		request.Body = nil
	}
}

func (conn *Connection) Closing() bool {
	return conn.closing
}
