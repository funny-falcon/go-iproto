package main

import (
	"encoding/binary"
	"github.com/funny-falcon/go-iproto"
	"github.com/funny-falcon/go-iproto/marshal"
	"github.com/funny-falcon/go-iproto/net/client"
	"github.com/funny-falcon/go-iproto/net/server"
	"log"
	"time"

	"flag"
	"os"
	"os/signal"
	"runtime/pprof"
)

var _ = log.Print
var _ = marshal.Read

const (
	OP_SUMTEST, OP_TEST iproto.RequestType = 1, 2
	RcError             iproto.RetCode     = 1
	CHKNUM                                 = 32
)

var le = binary.LittleEndian

var RootService = iproto.Route(rootService)

func rootService(r *iproto.Request) {
	switch r.Msg {
	case OP_TEST:
		OpTestService.Send(r)
	case OP_SUMTEST:
		SumTestService.Send(r)
	}
}

var OpTestService = iproto.SF(opTestService)

func opTestService(r *iproto.Request) {
	if len(r.Body) != 4 {
		r.Respond(RcError, nil)
	}
	num := le.Uint32(r.Body)
	result := make([]byte, 4)
	le.PutUint32(result, num)
	r.RespondBytes(iproto.RcOK, result)
}

var in_count = 0
var bad_count = 0

type OpTestReq struct {
	J uint32
}

func (r *OpTestReq) IMsg() iproto.RequestType {
	return OP_TEST
}

/*
func (r *OpTestReq) IWrite(w *marshal.Writer) {
	w.Uint32(r.J)
}
*/

type Res struct {
	J uint32
}

/*
func (r *Res) IRead(read *marshal.Reader) {
	r.J = read.Uint32()
	return
}
*/

//var SumTestService = iproto.BF{N: 512, Timeout: 2000 * time.Millisecond}.New(sumTestService)
var SumTestService = iproto.BF{N: 512, Timeout: 2000 * time.Millisecond}.New(sumTestService)

//var SumTestService = iproto.BF{N: 512}.New(sumTestService)

func sumTestService(cx *iproto.Context, req *iproto.Request) (iproto.RetCode, interface{}) {
	defer func() {
		if m := recover(); m != nil {
			log.Printf("PANICING %+v", m)
		}
	}()
	var sum uint32
	sums := make(chan uint32, 1)
	result := iproto.RcOK

	in_count++
	if in_count%10000 == 0 {
		log.Println("in count", in_count)
	}

	for beg, j := 0, 0; j < cap(sums); j++ {
		end := beg + (CHKNUM+j)/cap(sums)
		/*cx.GoInt(func(cx *iproto.Context, ji interface{}) {
		jj := ji.([2]int)
		beg, end := uint32(jj[0]), uint32(jj[1])
		*/

		mr := cx.NewMulti()
		mr.TimeoutFrom(ProxyTestService)

		var req OpTestReq
		for i := uint32(beg); i < uint32(end); i++ {
			req.J = i * i
			mr.Send(ProxyTestService, &req)
		}

		var s uint32
		for _, res := range mr.Results() {
			if res.Code != iproto.RcOK {
				mr.Cancel()
				result = RcError
				break
			}
			var i Res
			if err := res.Body.Read(&i); err != nil {
				log.Println(err)
				mr.Cancel()
				result = RcError
				break
			}
			s += i.J
		}
		sums <- s
		//}, [2]int{beg, end})
		beg = end
	}
	cx.WaitAll()
	close(sums)

	for s := range sums {
		sum += s
	}

	if result == RcError {
		bad_count++
		if bad_count%1000 == 0 {
			log.Println("bad count", bad_count)
		}
	}

	return 0, sum
}

var ProxyTestService iproto.EndPoint

var serverConf = server.Config{
	Network:  "tcp",
	Address:  ":8766",
	EndPoint: &RootService,
}

var recurConf = client.ServerConfig{
	Network:      "tcp",
	Address:      ":8765",
	Connections:  4,
	PingInterval: time.Hour,
	Timeout:      2000 * time.Millisecond,
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
	iproto.Run(ProxyTestService)

	self := serverConf.NewServer()
	self.Run()

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
