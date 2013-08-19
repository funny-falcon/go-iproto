package server

import (
	"net"
	"fmt"
	"sync"
	"log"
	nt "github.com/funny-falcon/go-iproto/net"
)

type Server struct {
	Config

	listener net.Listener

	closing bool
	stop chan bool
	connClosed chan uint64

	sync.Mutex
	conns map[uint64] *Connection
	currentId uint64
}

func (s *Server) Run() (err error) {
	if !s.EndPoint.Runned() {
		return fmt.Errorf("End point is not running %+v", s.EndPoint)
	}
	if s.listener, err = net.Listen(s.Network, s.Address); err != nil {
		return
	}

	s.connClosed = make(chan uint64)

	go s.listenLoop()
	go s.controlLoop()
	return
}

func (serv *Server) Stop() {
	serv.Lock()
	serv.closing = true
	serv.Unlock()
	serv.listener.Close()
	serv.stop <- true
}

func (serv *Server) controlLoop() {
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
			serv.Unlock()
			serv.closing = true
		}
	}
}

func (serv *Server) listenLoop() {
	for {
		conn, err := serv.listener.Accept()
		if err != nil {
			log.Printf("Accept on %s:%s failed with %v", serv.Network, serv.Address, err)
			serv.Lock()
			if serv.closing {
				serv.Unlock()
				break
			}
			serv.Unlock()
			continue
		}
		serv.Lock()
		if serv.closing {
			serv.Unlock()
			conn.Close()
			break
		}
		serv.currentId++
		connection := NewConnection(serv, conn.(nt.NetConn), serv.currentId)
		serv.conns[serv.currentId] = connection
		serv.Unlock()
	}
}
