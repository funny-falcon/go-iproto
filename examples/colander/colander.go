package main

import (
	"github.com/funny-falcon/go-iproto"
	"github.com/funny-falcon/go-iproto/net/client"
	"github.com/funny-falcon/go-iproto/net/server"
	"encoding/binary"
	"log"
	"time"

	"flag"
	"runtime/pprof"
	"os"
	"os/signal"
)

var _ = log.Print

const (
	OP_SUMTEST, OP_TEST iproto.RequestType = 1, 2
	RcError iproto.RetCode = 1
	CHKNUM = 32
)

type Service struct {
	Recur iproto.Service
	SumTest iproto.Service
}

var le = binary.LittleEndian

func (s *Service) SendWrapped(r *iproto.Request) {
	switch r.Msg {
	case OP_TEST:
		if !r.SetInFly(nil) {
			return
		}
		if len(r.Body) != 4 {
			r.Respond(RcError, nil)
		}
		num := le.Uint32(r.Body)
		result := make([]byte, 4)
		le.PutUint32(result, num)
		r.Respond(iproto.RcOK, result)
	case OP_SUMTEST:
		s.SumTest.Send(r)
	}
}

func (s *Service) Send(r *iproto.Request) {
	s.SendWrapped(r)
}

func (s *Service) Runned() bool {
	return s.Recur != nil
}

func (s *Service) DoSumTest(r *iproto.Request) {
	var wg iproto.WaitGroup
	var sum uint32
	result := iproto.RcOK

	wg.Init()
	bodies := make([]byte, 4*CHKNUM)
	for i:=uint32(0); i<CHKNUM; i++ {
		body := bodies[4*i:4*i+4]
		le.PutUint32(body, i*i)
		req := wg.Request(OP_TEST, body)
		s.Recur.Send(req)
	}

	for res := range wg.Results() {
		if res.Code != iproto.RcOK {
			wg.Cancel()
			result = RcError
		}
		sum += le.Uint32(res.Body)
	}

	body := make([]byte, 4)
	le.PutUint32(body, sum)
	r.Respond(result, body)
}

var colanderTest Service

var serverConf = server.Config {
	Network: "tcp",
	Address: ":8766",
	RetCodeLen: 4,
	EndPoint: &colanderTest,
}

var recurConf = client.ServerConfig {
	Network: "tcp",
	Address: ":8766",
	RetCodeLen: 4,
	Connections: 4,
	PingInterval: time.Hour,
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	recur := recurConf.NewServer()
	colanderTest.Recur = recur
	colanderTest.SumTest = iproto.NewParallelService(
		400,
		iproto.FuncService(colanderTest.DoSumTest),
	)

	self := serverConf.NewServer()
	self.Run()
	iproto.Run(recur)

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, os.Kill)
	<-ch

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}
