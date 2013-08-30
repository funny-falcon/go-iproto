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

var le = binary.LittleEndian

var RootService = iproto.FuncMiddleService(func(r *iproto.Request) {
	switch r.Msg {
	case OP_TEST:
		OpTestService.Send(r)
	case OP_SUMTEST:
		SumTestService.Send(r)
	}
})

var OpTestService = iproto.FuncEndService(func(r *iproto.Request) {
	if len(r.Body) != 4 {
		r.Respond(RcError, nil)
	}
	num := le.Uint32(r.Body)
	result := make([]byte, 4)
	le.PutUint32(result, num)
	r.Respond(iproto.RcOK, result)
})

var in_count = 0
var bad_count = 0
var SumTestService = iproto.NewParallelService(512, 100*time.Millisecond, func(cx *iproto.Context) {
	defer func() {
		if m := recover(); m != nil {
			log.Printf("PANICING %+v", m)
		}
	}()
	var sum uint32
	var sums [5]uint32
	st := CHKNUM / len(sums)
	result := iproto.RcOK

	in_count++
	if in_count % 10000 == 0 {
		log.Println("in count", in_count)
	}

	for j := range sums {
		cx.GoInt(func(cx *iproto.Context, ji interface{}) {
			var s uint32
			j := ji.(int)

			mr := cx.NewMulti()
			mr.TimeoutFrom(ProxyTestService)
			var body [4]byte
			mod := CHKNUM%len(sums)
			add := 0
			if mod+j >= len(sums) {
				add = (mod+j) % len(sums)
			}
			from := uint32(j*st) + uint32(add)
			to := from + uint32(st + (mod+j)/len(sums))
			for i:=from; i<to; i++ {
				le.PutUint32(body[:], i*i)
				req := mr.Request(OP_TEST, body[:])
				ProxyTestService.Send(req)
			}

			for _, res := range mr.Results() {
				if res.Code != iproto.RcOK {
					mr.Cancel()
					result = RcError
					//sum = ^uint32(0)
					break
				}
				s += le.Uint32(res.Body)
			}
			sums[j] = s
		}, j)
	}
	cx.WaitAll()

	for _, s := range sums {
		sum += s
	}

	if result == RcError {
		bad_count++
		if bad_count % 1000 == 0 {
			log.Println("bad count", bad_count)
		}
	}

	var body [4]byte
	le.PutUint32(body[:], sum)
	cx.Respond(0, body[:])
})

var ProxyTestService iproto.EndPoint

var serverConf = server.Config {
	Network: "tcp",
	Address: ":8766",
	RetCodeLen: 4,
	EndPoint: &RootService,
}

var recurConf = client.ServerConfig {
	Network: "tcp",
	Address: ":8765",
	RetCodeLen: 4,
	Connections: 4,
	PingInterval: time.Hour,
	Timeout: 100*time.Millisecond,
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

	ProxyTestService = recurConf.NewServer()

	self := serverConf.NewServer()
	self.Run()
	iproto.Run(ProxyTestService)

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
	log.Println()
}
