package main

import (
	"github.com/funny-falcon/go-iproto"
	"github.com/funny-falcon/go-iproto/net/client"
	"flag"
	"time"
	"fmt"
	"math"
	"runtime"
	"log"
)
var _ = log.Print
var _ = runtime.Gosched

type Accum struct {
	Count uint64
	Bad uint64
	Min, Max, Sum uint64
	Sum2 float64
}

func (a *Accum) Epoch(t time.Duration, good bool) {
	a.Count ++
	if !good {
		a.Bad++
	}
	d := uint64(t)
	a.Sum += d
	a.Sum2 += float64(d)*float64(d)
	if a.Min > d {
		a.Min = d
	}
	if a.Max < d {
		a.Max = d
	}
}

func (a *Accum) Accum(o *Accum) {
	a.Count += o.Count
	a.Bad += o.Bad
	a.Sum += o.Sum
	a.Sum2 += o.Sum2
	if a.Min > o.Min {
		a.Min = o.Min
	}
	if a.Max < o.Max {
		a.Max = o.Max
	}
}

func (a *Accum) String() string {
	cnt := float64(a.Count)
	return fmt.Sprintf("Count: %d Min: %f Max: %f Avg: %f  Stddef: %f Bad: %d", a.Count,
		float64(a.Min)/1e6, float64(a.Max)/1e6, float64(a.Sum)/cnt/1e6,
		math.Sqrt(a.Sum2/cnt - math.Pow(float64(a.Sum)/cnt, 2))/1e6,
		a.Bad)
}

type Epoch struct {
	iproto.Epoch
	*Accum
}

func (e *Epoch) Respond(res *iproto.Response) {
	e.Accum.Epoch(iproto.NowEpoch().Sub(e.Epoch), res.Valid())
}

func main() {
	var n, c, p, g, batch int
	var actioni int
	var h string
	flag.IntVar(&n, "n", 100000, "Num of Requests")
	flag.IntVar(&c, "c", 1, "Num of connections")
	flag.IntVar(&g, "g", 1, "Num of goroutines")
	flag.IntVar(&batch, "b", 64, "Size of batch")
	flag.StringVar(&h, "h", "127.0.0.1", "colander host")
	flag.IntVar(&p, "p", 8765, "Colander port")
	flag.IntVar(&actioni, "a", 1, "Action: 1 - sumtest, 2 - echo")
	flag.Parse()

	action := iproto.RequestType(actioni)


	conf := client.ServerConfig{
		Network: "tcp",
		Address: fmt.Sprintf("%s:%d", h, p),
		Connections: c,
		RetCodeLen: 4,
		PingInterval: 1*time.Second,
		//Timeout: time.Second,
	}

	serv := conf.NewServer()

	iproto.Run(serv)

	var point iproto.EndPoint
	point = serv

	accum := Accum{Min: ^uint64(0)}
	var body []byte
	if action == 1 {
		body = []byte{}
	} else if action == 2 {
		body = []byte("asdf")
	} else {
		log.Panicf("Action should be 1 or 2")
	}
	fmt.Println("About to Send")

	start := time.Now()

	accums := make(chan *Accum, 1)
	for j:=uint32(1); j<=uint32(g); j++ {
		go func(j uint32){
			var cx iproto.Context
			locaccum := Accum{Min: ^uint64(0)}
			for i:=0; i<n; i+=batch {
				epochs := make([]iproto.Epoch, batch)
				mr := cx.NewMulti()
				for j:=0; j < batch && i+j<n; j++ {
					epochs[j] = iproto.NowEpoch()
					req := mr.Request(action, body)
					point.Send(req)
				}
				for _, res := range mr.Results() {
					locaccum.Epoch(iproto.NowEpoch().Sub(epochs[res.Id]), res.Valid())
				}
			}
			accums <- &locaccum
		}(j)
	}

	fmt.Println("Sent", &accum)
	defer func() {
		fmt.Println("Recv", &accum)
		t := time.Now().Sub(start)
		fmt.Printf("Stop %v rps: %f\n", t, float64(accum.Count) / (float64(t)/1.0e9))
	}()
	for j:=uint32(1); j<=uint32(g); j++ {
		locaccum := <-accums
		accum.Accum(locaccum)
	}
	point.Stop()

}
