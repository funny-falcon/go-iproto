package server

import (
	"fmt"
	"github.com/funny-falcon/go-iproto"
	nt "github.com/funny-falcon/go-iproto/net"
	"log"
	"net"
	"sync"
)

type Server struct {
	Config
	Running chan bool

	listener net.Listener

	closing    bool
	stop       chan bool
	connClosed chan uint64

	sync.Mutex
	conns     map[uint64]*Connection
	currentId uint64
}

func (cfg *Config) NewServer() (serv *Server) {
	serv = &Server{
		Config: *cfg,
	}
	if serv.RCMap != nil {
		for k, v := range serv.RCMap {
			if k&iproto.RcKindMask != iproto.RcInternal {
				log.Panicf("Should map internal ret code %x to non internal code", k)
			}
			if v&iproto.RcKindMask == iproto.RcInternal {
				log.Panicf("Should not map internal ret code %d to internal ret code %d", k, v)
			}
		}
	}

	serv.Running = make(chan bool)
	serv.stop = make(chan bool, 1)
	serv.connClosed = make(chan uint64)
	serv.conns = make(map[uint64]*Connection)

	return
}

func (serv *Server) Run() (err error) {
	if !serv.EndPoint.Runned() {
		return fmt.Errorf("End point is not running %+v", serv.EndPoint)
	}
	if serv.listener, err = net.Listen(serv.Network, serv.Address); err != nil {
		return
	}
	log.Println("Binded to", serv.Address)

	go serv.listenLoop()
	go serv.controlLoop()
	return
}

func (serv *Server) Stop() {
	serv.Lock()
	serv.closing = true
	serv.stop <- true
	serv.Unlock()
	serv.listener.Close()
}

func (serv *Server) controlLoop() {
	defer close(serv.Running)
	for {
		select {
		case id := <-serv.connClosed:
			serv.Lock()
			delete(serv.conns, id)
			if serv.closing && len(serv.conns) == 0 {
				serv.Unlock()
				return
			}
			serv.Unlock()
		case <-serv.stop:
			serv.Lock()
			for _, conn := range serv.conns {
				conn.Stop()
			}
			if len(serv.conns) == 0 {
				serv.Unlock()
				return
			}
			serv.Unlock()
		}
	}
}

func (serv *Server) listenLoop() {
	for {
		conn, err := serv.listener.Accept()
		if err != nil {
			serv.Lock()
			if serv.closing {
				serv.Unlock()
				break
			} else {
				log.Printf("Accept on %s:%s failed with %v", serv.Network, serv.Address, err)
			}
			serv.Unlock()
			continue
		}
		log.Printf("Accepted %s on %s", conn.RemoteAddr(), conn.LocalAddr())
		serv.Lock()
		if serv.closing {
			serv.Unlock()
			conn.Close()
			break
		}
		serv.currentId++
		connection := NewConnection(serv, conn.(nt.NetConn), serv.currentId)
		serv.conns[serv.currentId] = connection
		connection.Run()
		serv.Unlock()
	}
}
