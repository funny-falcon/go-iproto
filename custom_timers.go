package iproto

import (
	"time"
	"github.com/funny-falcon/go-iproto/util"
	"github.com/funny-falcon/go-fastheap"
	"log"
	"sync"
	"runtime"
)

var _ = log.Print

type timeout interface {
	fastheap.IntValue
	Expire()
}

var heaps = make([]heap, runtime.GOMAXPROCS(-1))
var heapsI util.Atomic

func init() {
	for i:=0; i<cap(heaps); i++ {
		heaps[i].Init()
	}
}

type heapRef uint32
func (h heapRef) Insert(t timeout) {
	heaps[h].Insert(t)
}

func (h heapRef) Remove(t timeout) {
	heaps[h].Remove(t)
}

func getHeap() heapRef {
	return heapRef(heapsI.Incr() % util.Atomic(len(heaps)))
}

type heap struct {
	sync.Mutex
	heap fastheap.IntHeap
	changed chan bool
}

func (h *heap) Init() {
	h.changed = make(chan bool, 1)
	go h.Loop()
}

func (h *heap) Insert(t timeout) {
	if t.Index() != 0 {
		panic("timeout insert: tm.index() != 0")
	}

	h.Lock()
	defer h.Unlock()

	h.heap.Insert(t)

	select {
	case h.changed<- true:
	default:
	}
}

func (h *heap) Remove(t timeout) {
	if t.Index() == 0 {
		return
	}

	h.Lock()
	defer h.Unlock()

	h.heap.Remove(t)

	select {
	case h.changed<- true:
	default:
	}
}

func (h *heap) Loop() {
	deadline := NowEpoch().Add(time.Hour)
	t := time.NewTimer(deadline.Remains())
	for {
		if newDeadline := h.checkExpired(); newDeadline != deadline {
			deadline = newDeadline
			t.Reset(deadline.Remains())
		}
		select {
		case <-t.C:
		case <-h.changed:
		}
	}
}

func (h *heap) checkExpired() Epoch {
	now := NowEpoch()
	h.Lock()
	defer h.Unlock()
	for {
		ref, v, ok := h.heap.Top()
		if !ok {
			return now.Add(time.Hour)
		}
		if Epoch(v).Sub(now) >= 0 {
			return Epoch(v)
		}
		h.heap.Pop()
		h.Unlock()
		ref.(timeout).Expire()
		h.Lock()
	}
}
