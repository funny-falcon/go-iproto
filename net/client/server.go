package client

import (
	"github.com/funny-falcon/go-iproto"
	"github.com/funny-falcon/go-iproto/net/client/connection"
	"log"
	"time"
)

type SConf struct {
	Name string
	Connections int
}

type conf struct {
	connection.CConf
	SConf
}

type actionKind int
const (
	setServ = actionKind(iota+1)
	setReadTimeout
	setWriteTimeout
)

type action struct {
	kind       actionKind
	servs      int
	timeout    time.Duration
}

type Server struct {
	conf

	iproto.SimplePoint

	connections map[uint64]*connection.Connection
	curId       uint64
	needConns   int
	dialing     int
	established int
	dying       int

	connErr     chan connection.Error
	actions     chan action
	exiting     bool
}

var _ iproto.EndPoint = (*Server)(nil)

func (cfg *ServerConf) NewServer() (serv *Server) {
	cfg.SetDefaults()

	serv = &Server{
		conf: conf{
			SConf: SConf{
				Name:           cfg.Name,
				Connections:    cfg.Connections,
			},
			CConf: connection.CConf{
				Network:      cfg.Network,
				Address:      cfg.Address,
				PingInterval: cfg.PingInterval,
				ReadTimeout:  cfg.ReadTimeout,
				WriteTimeout: cfg.WriteTimeout,
				RetCodeLen:   cfg.RetCodeLen,
			},
		},
		connErr: make(chan connection.Error, 4),
		actions: make(chan action, 1),
		connections: make(map[uint64]*connection.Connection),
	}

	serv.SimplePoint.Init()
	serv.ConnErr = serv.connErr

	return
}

func (serv *Server) Run(ch chan *iproto.Request) {
	serv.SetChan(ch)
	serv.needConns = serv.Connections
	serv.fixConnections()
	go serv.Loop()
}

func (serv *Server) fixConnections() {
	needConn := serv.needConns - serv.dialing + serv.established
	for ; needConn > 0; needConn-- {
		serv.curId++
		conn := connection.NewConnection(&serv.CConf, serv.curId)
		serv.connections[serv.curId] = conn
		serv.RunChild(conn)
		serv.dialing++
	}
	if needConn < 0 {
		for _, conn := range serv.connections {
			switch conn.State {
			case connection.CsDialing:
				conn.Stop()
				serv.dialing--
				needConn++
			case connection.CsConnected:
				conn.Stop()
				serv.established--
				needConn++
			}
			if needConn == 0 {
				break
			}
		}
	}
}

func (serv *Server) Name() string {
	return serv.conf.Name
}

func (serv *Server) Loop() {
	for {
		select {
		case <-serv.ExitChan():
			serv.needConns = 0
			serv.exiting = true
			break
		case connErr := <-serv.connErr:
			serv.onConnError(connErr)
		case action := <-serv.actions:
			serv.onAction(action)
		}

		if serv.exiting && serv.established + serv.dialing == 0 {
			break
		}
	}
}

func (serv *Server) onAction(action action) {
	switch action.kind {
	case setServ:
		serv.Connections = action.servs
		serv.needConns = action.servs
		serv.fixConnections()
	case setReadTimeout:
		serv.ReadTimeout = action.timeout
		for _, conn := range serv.connections  {
			conn.SetReadTimeout(action.timeout)
		}
	case setWriteTimeout:
		serv.WriteTimeout = action.timeout
		for _, conn := range serv.connections  {
			conn.SetWriteTimeout(action.timeout)
		}
	}
}

func (serv *Server) onConnError(connErr connection.Error) {
	conn := connErr.Conn
	switch connErr.When {
	case connection.Dial:
		serv.dialing--
		if connErr.Error == nil {
			log.Printf("%s: established connection %v -> %v", serv.conf.Name, conn.LocalAddr(), conn.RemoteAddr())
			serv.established++
		} else {
			log.Printf("%s: could not connect to %v", serv.conf.Name, conn.LocalAddr(), conn.RemoteAddr())
			if _, ok := serv.connections[conn.Id]; !ok {
				log.Panicf("Unknown connection failed %+v", conn)
			}
			delete(serv.connections, conn.Id)
			serv.fixConnections()
		}
	case connection.Write:
		log.Printf("%s: write side closed %v -> %v", serv.conf.Name, conn.LocalAddr(), conn.RemoteAddr())
		serv.established--
		serv.dying++
		serv.fixConnections()
	case connection.Read:
		log.Printf("%s: read side closed %v -> %v", serv.conf.Name, conn.LocalAddr(), conn.RemoteAddr())
		serv.dying--
		if _, ok := serv.connections[conn.Id]; !ok {
			log.Panicf("Unknown connection failed %+v", conn)
		}
		delete(serv.connections, conn.Id)
		serv.fixConnections()
	}
}

func (serv *Server) SetConnections(n int) {
	serv.actions <- action{ kind: setServ, servs: n }
}

func (serv *Server) SetReadTimeout(timeout time.Duration) {
	serv.actions <- action{ kind: setReadTimeout, timeout: timeout }
}

func (serv *Server) SetWriteTimeout(timeout time.Duration) {
	serv.actions <- action{ kind: setWriteTimeout, timeout: timeout }
}
