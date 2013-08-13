package net_client

import (
	"github.com/funny-falcon/go-iproto"
	"github.com/funny-falcon/go-iproto/net_client/connection"
	"github.com/funny-falcon/go-iproto/util"
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
	exit = actionKind(iota+1)
	setServ
	setReadTimeout
	setWriteTimeout
	setPingInterval
)

type action struct {
	kind       actionKind
	servs      int
	timeout    time.Duration
}

type Server struct {
	conf

	iproto.SimplePoint

	connections util.IdGenerator
	needConns   int
	dialing     int
	established int
	dying       int

	connErr   chan connection.Error

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
	}

	serv.connections.Init(0, ^uint32(0))

	serv.CConf.ConnErr = serv.connErr

	return
}

func (serv *Server) IProtoRun(requests chan *iproto.Request) {
	serv.SimplePoint = requests
	serv.needConns = serv.Connections
	serv.fixConnections()
	go serv.Loop()
}

func (serv *Server) IProtoStop() {
	serv.actions <- action{ kind: exit }
}

func (serv *Server) fixConnections() {
	for ;serv.dialing + serv.established < serv.needConns; serv.dialing++ {
		var id uint32
		var err error
		if id, err = serv.connections.Next(); err != nil {
			log.Panic("Could not generate ID for connection", err)
		}
		conn := connection.NewConnection(&serv.CConf, id)
		serv.connections.Set(id, conn)
		conn.IProtoRun(serv.SimplePoint)
	}
	if serv.dialing + serv.established > serv.needConns {
		for _, i := range serv.connections.Holded() {
			conn := i.(connection.Connection)
			conn.Control <- connection.Control{ Kind: connection.CloseWrite }
		}
	}
}

func (serv *Server) notifyConnections(kind connection.ConnControlKind, duration time.Duration) {
	for _, i := range serv.connections.Holded() {
		con := i.(connection.Connection)
		con.Control <- connection.Control{ Kind: kind, Duration: duration }
	}
}

func (serv *Server) Name() string {
	return serv.conf.Name
}

func (serv *Server) Loop() {
	for {
		select { case connErr := <-serv.connErr:
			serv.onConnError(connErr)
		case action := <-serv.actions:
			switch action.kind {
			case setServ:
				serv.Connections = action.servs
				serv.needConns = action.servs
			case exit:
				serv.needConns = 0
				serv.exiting = true
			case setReadTimeout:
				serv.CConf.ReadTimeout = action.timeout
				serv.notifyConnections(connection.ReadTimeout, action.timeout)
			case setWriteTimeout:
				serv.CConf.WriteTimeout = action.timeout
				serv.notifyConnections(connection.WriteTimeout, action.timeout)
			case setPingInterval:
				serv.CConf.PingInterval = action.timeout
				serv.notifyConnections(connection.PingInterval, action.timeout)
			}
		}

		if serv.exiting && serv.established + serv.dialing == 0 {
			break
		}
	}
}

func (serv *Server) onConnError(connErr connection.Error) {
	switch connErr.When {
	case connection.Dial:
		serv.dialing--
		if connErr.Error == nil {
			serv.established++
		} else {
			if serv.connections.Remove(connErr.Conn.Id) == nil {
				log.Panicf("Unknown connection failed %+v", connErr.Conn)
			}
			serv.fixConnections()
		}
	case connection.Write:
		serv.established--
		serv.dying++
		serv.fixConnections()
	case connection.Read:
		serv.dying--
		if serv.connections.Remove(connErr.Conn.Id) == nil {
			log.Panicf("Unknown connection failed %+v", connErr.Conn)
		}
	}
}

func (serv *Server) SetConnections(n int) {
	serv.actions <- action{ kind: setServ, servs: n }
}
