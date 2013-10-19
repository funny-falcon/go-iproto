package main

import (
	"flag"
	"fmt"
	"github.com/funny-falcon/go-iproto"
	"github.com/funny-falcon/go-iproto/net/client"
	"github.com/funny-falcon/go-iproto/sbox"
	"reflect"
	"sync"
	"time"
)

type TStruct struct {
	Id        int32
	Month     int32
	Day       int32
	Owner     int32
	SubjectId int32

	Email      string
	Nick       string
	FirstName  []byte
	LastName   []byte
	MaidenName []byte

	Year   int32
	Sex    int32
	Region int32

	AvatarData []byte

	MyAccess            int32
	AccessJournal       int32
	AccessUmask         int32
	AccessAntiUmask     int32
	AccessDefaultMask   int32
	AccessCanChangeMask int32

	Flags, Flags2, Flags3, Flags4 uint32

	CreateTime, ModifyTime uint32
}

var n = flag.Int("n", 1000000, "Number of select")
var g = flag.Int("g", 1, "Number of gorouines")

func main() {
	flag.Parse()

	tuple := TStruct{
		Id: 12345, Email: "hello@worl.d",
		Nick:       "asdffdsa",
		FirstName:  []byte("Юрий"),
		LastName:   []byte("Соколов"),
		MaidenName: []byte("Соколов"),
		AvatarData: []byte("some where in a dark"),
	}

	box := client.ServerConfig{Address: "localhost:33010"}.NewServer()
	iproto.Run(box)

	res := iproto.Call(box, sbox.StoreReq{Space: 0, Return: true, Tuple: tuple})
	if res.Code != sbox.RcOK {
		fmt.Printf("Store error: %x %s\n", res.Code, res.Body)
		return
	}
	var c uint32
	res.Body.Read(&c)
	fmt.Printf("Store %d [% x]\n", c, res.Body)

	res = iproto.Call(box, sbox.SelectReq{Space: 0, Index: 0, Limit: -1, Keys: int32(12345)})
	if res.Code != sbox.RcOK {
		fmt.Printf("Select error: %x %s\n", res.Code, res.Body)
		return
	}
	var tuples []TStruct
	if _, _, err := sbox.ReadMany(res.Body, &tuples); err != nil {
		fmt.Printf("Select parse error: %v\n", err)
		return
	}
	if !reflect.DeepEqual(tuple, tuples[0]) {
		fmt.Printf("Not equal %+v %+v\n", tuple, tuples[0])
	}

	res = iproto.Call(box, sbox.SelectReq{Space: 0, Index: 1, Limit: -1, Keys: "hello@worl.d"})
	if res.Code != sbox.RcOK {
		fmt.Printf("Select error: %x %s\n", res.Code, res.Body)
		return
	}
	if _, _, err := sbox.ReadMany(res.Body, &tuples); err != nil {
		fmt.Printf("Select parse error: %v\n", err)
		return
	}
	if !reflect.DeepEqual(tuple, tuples[0]) {
		fmt.Printf("Not equal %+v %+v\n", tuple, tuples[0])
	}

	add := *n / *g
	mod := *n % *g
	if mod == 0 {
		mod = *g + 1
	}

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *g; i++ {
		if i == mod {
			add++
		}
		wg.Add(1)
		go func(n int) {
			cx := iproto.Context{}
			for i := 0; i < n; i++ {
				//var tuples []TStruct
				var tupel TStruct
				res := cx.Call(box, sbox.SelectReq{
					Space: 0, Index: 0, Limit: -1,
					Keys: int32(12345),
				})
				//sbox.ReadMany(res.Body, &tuples)
				sbox.ReadMany(res.Body, &tupel)
				/*if !reflect.DeepEqual(tupel, tuple) {
					panic("not equal")
				}*/
			}
			cx.Done()
			wg.Done()
		}(add)
	}
	wg.Wait()
	stop := time.Now()
	fmt.Printf("%d selects with concurency %d lasts %v - %frps\n",
		*n, *g, stop.Sub(start),
		float64(*n)/float64(stop.Sub(start))*1e9)
}
