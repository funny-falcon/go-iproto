package iproto

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var _ = log.Print

type Expirator interface {
	Expire()
	Timer() *Timer
}

type Timer struct {
	h uint32
	i uint32
}

func (t *Timer) After(d time.Duration, e Expirator) {
	var h *tHeap
	if t.h == 0 {
		h = nextHeap()
		t.h = h.i
	} else {
		h = getHeap(t.h)
	}
	h.m.Lock()
	top := h.push(e, NowEpoch().Add(d))
	if top {
		h.c.Signal()
	}
	h.m.Unlock()
}

func (t *Timer) Stop() {
	if h := t.h; h > 0 {
		heap := getHeap(h)
		heap.m.Lock()
		if i := t.i; i > 0 {
			heap.remove(i)
		}
		heap.m.Unlock()
	}
}

var heaps = []*tHeap{newHeap(1), newHeap(2)}
var heapM sync.Mutex
var heapI uint32
var heapN uint32 = 2

func nextHeap() *tHeap {
	i := atomic.AddUint32(&heapI, 1)
	if i%10000 == 0 {
		heapM.Lock()
		n := runtime.GOMAXPROCS(-1)
		if n == 1 {
			n = 2
		} else {
			n *= 8
		}
		for len(heaps) < n {
			heaps = append(heaps, newHeap(uint32(len(heaps)+1)))
		}
		heapN = uint32(n)
		heapM.Unlock()
	}
	return heaps[i%heapN]
}

func getHeap(h uint32) *tHeap {
	return heaps[h-1]
}

type htindex uint32
type tItem struct {
	x          Expirator
	e          Epoch
	upi, downl uint16
	up         uint32
	down       [4]uint32
}

type tHeap struct {
	m    sync.Mutex
	c    sync.Cond
	len  int
	i    uint32
	top  uint32
	size uint32
	free uint32
	h    []tItem
}

func newHeap(i uint32) *tHeap {
	h := &tHeap{i: i}
	h.c.L = &h.m
	h.h = make([]tItem, 1, 256)
	go h.loop()
	return h
}

func (h *tHeap) loop() {
	t := time.AfterFunc(time.Hour, h.c.Signal)
	h.m.Lock()
Loop:
	for {
		reCheck := false
		now := NowEpoch()
		for h.size != 0 {
			if h.h[h.top].e >= now {
				if reCheck {
					continue Loop
				}
				t.Reset(time.Duration(h.h[h.top].e - now))
				break
			}
			x := h.h[h.top].x
			h.remove(h.top)
			h.m.Unlock()
			if x != nil {
				x.Expire()
			}
			h.m.Lock()
			reCheck = true
		}
		if h.size == 0 {
			t.Reset(time.Hour)
		}
		h.c.Wait()
	}
}

func (h *tHeap) push(x Expirator, e Epoch) bool {
	var it *tItem
	t := x.Timer()
	if h.free != 0 {
		t.i = h.free
		it = &h.h[t.i]
		h.free = it.up
		*it = tItem{x: x, e: e}
	} else {
		t.i = uint32(len(h.h))
		h.h = append(h.h, tItem{x: x, e: e})
		it = &h.h[t.i]
	}
	h.size++
	if h.top == 0 {
		h.top = t.i
		return true
	} else if it.e <= h.h[h.top].e {
		h.putUnder(t.i, h.top)
		top := it.e < h.h[h.top].e
		h.top = t.i
		return top
	} else {
		h.putUnder(h.top, t.i)
		return false
	}
}

func (h *tHeap) remove(at uint32) {
	t := &h.h[at]
	t.x.Timer().i = 0
	h.size--
	var downi uint32
	switch t.downl {
	case 0:
		if at == h.top {
			h.top = 0
		} else {
			up := &h.h[t.up]
			switch t.upi {
			case 0:
				up.down[0] = up.down[1]
				h.h[up.down[1]].upi--
				fallthrough
			case 1:
				up.down[1] = up.down[2]
				h.h[up.down[2]].upi--
				fallthrough
			case 2:
				up.down[2] = up.down[3]
				h.h[up.down[3]].upi--
				fallthrough
			case 3:
				up.down[3] = 0
			}
			up.downl--
		}
		goto Exit
	case 1:
		downi = t.down[0]
	case 2:
		if h.h[t.down[0]].e <= h.h[t.down[1]].e {
			h.putUnder(t.down[0], t.down[1])
			downi = t.down[0]
		} else {
			h.putUnder(t.down[1], t.down[0])
			downi = t.down[1]
		}
	case 3:
		e0 := h.h[t.down[0]].e
		e1 := h.h[t.down[1]].e
		e2 := h.h[t.down[2]].e
		if e1 < e0 {
			t.down[0], t.down[1] = t.down[1], t.down[0]
			e1, e0 = e0, e1
		}
		if e2 < e1 {
			t.down[1], t.down[2] = t.down[2], t.down[1]
			e2, e1 = e1, e2
			if e1 < e0 {
				t.down[0], t.down[1] = t.down[1], t.down[0]
			}
		}
		h.putUnder(t.down[1], t.down[2])
		h.putUnder(t.down[0], t.down[1])
		downi = t.down[0]
	case 4:
		h.sort4(&t.down)
		downi = t.down[0]
	}
	if at == h.top {
		h.top = downi
		down := &h.h[downi]
		down.up = 0
		down.upi = 0
	} else {
		down := &h.h[downi]
		down.up = t.up
		down.upi = t.upi
		h.h[t.up].down[t.upi] = downi
	}
Exit:
	*t = tItem{up: h.free}
	h.free = at
}

func (h *tHeap) putUnder(upi, downi uint32) {
	up := &h.h[upi]
	down := &h.h[downi]
	down.up = upi
	if up.downl == 4 {
		h.sort4(&up.down)
		up.down[3] = 0
		up.down[2] = 0
		up.down[1] = downi
		h.h[up.down[0]].upi = 0
		down.upi = 1
		up.downl = 2
	} else {
		up.down[up.downl] = downi
		down.upi = up.downl
		up.downl++
	}
}

func (h *tHeap) sort4(down *[4]uint32) {
	e0 := h.h[down[0]].e
	e1 := h.h[down[1]].e
	e2 := h.h[down[2]].e
	e3 := h.h[down[3]].e
	if e1 < e0 {
		down[0], down[1] = down[1], down[0]
		e1, e0 = e0, e1
	}
	if e2 < e1 {
		down[1], down[2] = down[2], down[1]
		e2, e1 = e1, e2
		if e1 < e0 {
			down[0], down[1] = down[1], down[0]
			e1, e0 = e0, e1
		}
	}
	if e3 < e2 {
		down[2], down[3] = down[3], down[2]
		e3, e2 = e2, e3
		if e2 < e1 {
			down[1], down[2] = down[2], down[1]
			e2, e1 = e1, e2
			if e1 < e0 {
				down[0], down[1] = down[1], down[0]
			}
		}
	}
	h.putUnder(down[2], down[3])
	h.putUnder(down[1], down[2])
	h.putUnder(down[0], down[1])
}
