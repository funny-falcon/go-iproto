package client

import (
	"sync"

	"github.com/funny-falcon/go-iproto"
)

type Auto struct {
	Config  *ServerConfig
	servers map[string]*Server
	sync.Mutex
}

func (a *Auto) Get(addr string) (serv *Server) {
	if a.servers == nil {
		a.Lock()
		if a.servers == nil {
			a.servers = make(map[string]*Server, len(a.servers))
		}
		a.Unlock()
	}
	if serv = a.servers[addr]; serv == nil {
		a.Lock()
		if serv = a.servers[addr]; serv == nil {
			conf := *a.Config
			conf.Address = addr
			serv = conf.NewServer()
			iproto.Run(serv)
			newServs := make(map[string]*Server, len(a.servers))
			newServs[addr] = serv
			for a, s := range a.servers {
				newServs[a] = s
			}
			a.servers = newServs
		}
		a.Unlock()
	}
	return
}

func (a *Auto) Stop() {
	a.Lock()
	if a.servers != nil {
		for _, serv := range a.servers {
			serv.Stop()
		}
		a.servers = nil
	}
	a.Unlock()
}
