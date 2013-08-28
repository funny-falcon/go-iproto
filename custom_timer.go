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
}

type Timer struct {
	E    Expirator
	i, h uint32
}

func (t *Timer) After(d time.Duration) {
	var h *tHeap
	if t.h == 0 {
		h = nextHeap()
		t.h = h.i
	} else {
		h = getHeap(t.h)
	}
	h.m.Lock()
	top := h.push(t, NowEpoch().Add(d))
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
		n := runtime.GOMAXPROCS(-1) * 4
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

type tItem struct {
	e Epoch
	t *Timer
}

type tHeap struct {
	m sync.Mutex
	c *sync.Cond
	i uint32
	h []tItem
}

func newHeap(i uint32) *tHeap {
	h := &tHeap{i: i}
	h.c = sync.NewCond(&h.m)
	h.h = make([]tItem, 3, 256)
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
		for len(h.h) > 3 {
			top := h.h[3]
			if top.e >= now {
				if reCheck {
					continue Loop
				}
				t.Reset(time.Duration(top.e - now))
				break
			}
			h.pop()
			h.m.Unlock()
			if top.t.E != nil {
				top.t.E.Expire()
			}
			h.m.Lock()
			reCheck = true
		}
		if len(h.h) == 3 {
			t.Reset(time.Hour)
		}
		h.c.Wait()
	}
}

func (h *tHeap) push(t *Timer, e Epoch) bool {
	l := uint32(len(h.h))
	t.i = l
	h.h = append(h.h, tItem{t: t, e: e})
	if l > 3 {
		h.up(l)
	}
	return t.i == 3
}

func (h *tHeap) pop() {
	l := len(h.h) - 1
	t := &h.h[3]
	t.t.i = 0
	if l > 3 {
		*t = h.h[l]
		t.t.i = 3
		h.chomp()
		h.down(3)
	} else {
		h.chomp()
	}
}

func (h *tHeap) remove(i uint32) {
	l := uint32(len(h.h) - 1)
	t := &h.h[i]
	t.t.i = 0
	if i < l {
		*t = h.h[l]
		t.t.i = i
		h.chomp()
		if !h.up(i) {
			h.down(i)
		}
	} else {
		h.chomp()
	}
}

func (h *tHeap) chomp() {
	l := len(h.h) - 1
	h.h[l] = tItem{}
	h.h = h.h[:l]
}

func (th *tHeap) up(i uint32) (up bool) {
	h := th.h
	t := h[i]
	for {
		j := i/4 + 2
		if j == 2 || h[j].e <= t.e {
			break
		}
		up = true
		h[i] = h[j]
		h[i].t.i = i
		h[j] = t
		t.t.i = j
		i = j
	}
	return
}

func (th *tHeap) down(i uint32) {
	h := th.h
	t := h[i]
	l := uint32(len(h))
	for {
		j := (i - 2) * 4
		j2 := j + 2
		if j >= l {
			break
		}
		e := h[j].e
		if j2 < l {
			e1 := h[j+1].e
			if e1 < e {
				e = e1
				j++
			}
			e2 := h[j2].e
			if j2+1 < l {
				e3 := h[j2+1].e
				if e3 < e2 {
					e2 = e3
					j2++
				}
			}
			if e2 < e {
				e = e2
				j = j2
			}
		} else if j+1 < l {
			e1 := h[j+1].e
			if e1 < e {
				e = e1
				j++
			}
		}
		if e >= t.e {
			break
		}
		h[i] = h[j]
		h[i].t.i = i
		h[j] = t
		t.t.i = j
		i = j
	}
}
