package iproto

import (
	"time"
	"github.com/funny-falcon/go-iproto/util"
	"log"
	"sync"
	"runtime"
)

var _ = log.Print

type CDeadline struct {
	basic BasicResponder
	state util.Atomic
	heap *CHeap
	send, recv int32
}

var heaps = make([]CHeap, runtime.GOMAXPROCS(-1))
var heapsI util.Atomic
func init() {
	for i:=0; i<cap(heaps); i++ {
		heaps[i].Init()
	}
}

func wrapInCDeadline(r *Request) {
	if r.Deadline.Zero() {
		return
	}
	d := CDeadline{}
	d.Wrap(r)
}

func (d *CDeadline) Wrap(r *Request) {
	if r.Deadline.Zero() {
		return
	}

	r.canceled = make(chan bool, 1)

	now := NowEpoch()
	recvRemains := r.Deadline.Sub(now)
	sendRemains := recvRemains - r.WorkTime
	d.basic.Chain(r)

	if sendRemains < 0 {
		d.sendExpired()
		return
	}

	d.heap = &heaps[int(heapsI.Incr()) % len(heaps)]
	d.heap.Insert(sendTimeout{d})
	d.heap.Insert(recvTimeout{d})
}

func (d *CDeadline) sendExpired() {
	r := d.basic.Request
	if r != nil && r.expireSend() {
		d.state.Store(dsCanceling)
		r.doCancel()
		res := Response { Id: r.Id, Msg: r.Msg, Code: RcSendTimeout }
		if prev := d.basic.Unchain(); prev != nil {
			prev.Respond(res)
		}
	}
}

func (d *CDeadline) recvExpired() {
	r := d.basic.Request
	if r != nil && r.goingToCancel() {
		d.state.Store(dsCanceling)
		r.doCancel()
		res := Response { Id: r.Id, Msg: r.Msg, Code: RcRecvTimeout }
		if prev := d.basic.Unchain(); prev != nil {
			prev.Respond(res)
		}
	}
}

func (d *CDeadline) Respond(res Response) {
	d.state.Store(dsResponding)
	d.heap.Remove(sendTimeout{d})
	d.heap.Remove(recvTimeout{d})
	prev := d.basic.Unchain()
	if prev != nil {
		prev.Respond(res)
	}
}

func (d *CDeadline) Cancel() {
	d.heap.Remove(sendTimeout{d})
	d.heap.Remove(recvTimeout{d})
	if !d.state.Is(dsCanceling) {
		prev := d.basic.Unchain()
		if prev != nil {
			prev.Cancel()
		}
	}
}

type timeout interface {
	epoch() Epoch
	index() int32
	setIndex(int32)
	expire()
}

type sendTimeout struct {
	*CDeadline
}

func (s sendTimeout) epoch() Epoch {
	d := s.CDeadline
	req := d.basic.Request
	if req == nil {
		return 0
	}
	return req.Deadline - Epoch(req.WorkTime)
}
func (s sendTimeout) index() int32 {
	d := s.CDeadline
	return d.send - 1
}
func (s sendTimeout) setIndex(i int32) {
	d := s.CDeadline
	d.send = i + 1
}

func (s sendTimeout) expire() {
	d := s.CDeadline
	d.sendExpired()
}

type recvTimeout struct {
	*CDeadline
}

func (s recvTimeout) epoch() Epoch {
	d := s.CDeadline
	req := d.basic.Request
	if req == nil {
		return 0
	}
	return req.Deadline
}
func (s recvTimeout) index() int32 {
	d := s.CDeadline
	return d.recv - 1
}
func (s recvTimeout) setIndex(i int32) {
	d := s.CDeadline
	d.recv = i + 1
}

func (s recvTimeout) expire() {
	d := s.CDeadline
	d.recvExpired()
}

/**************************************/

type heapItem struct {
	timeout
	Epoch
}
type CHeap struct {
	sync.Mutex
	heap []heapItem
	changed chan bool
}

func (h *CHeap) Init() {
	h.heap = make([]heapItem, 0, 128)
	h.changed = make(chan bool, 1)
	go h.Loop()
}

func (h *CHeap) Insert(t timeout) {
	if t.index() != -1 {
		panic("timeout insert: tm.index() != -1")
	}
	h.Lock()
	defer h.Unlock()
	h.insert(t)
	select {
	case h.changed<- true:
	default:
	}
}

func (h *CHeap) Remove(t timeout) {
	if t.index() == -1 {
		return
	}
	h.Lock()
	defer h.Unlock()
	h.remove(t)
	select {
	case h.changed<- true:
	default:
	}
}

func (h *CHeap) Loop() {
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

func (h *CHeap) checkExpired() Epoch {
	now := NowEpoch()
	h.Lock()
	defer h.Unlock()
	for len(h.heap) > 0 {
		t := h.heap[0]
		if t.Sub(now) >= 0 {
			break
		}
		h.pop()
		h.Unlock()
		t.expire()
		h.Lock()
	}
	if len(h.heap) > 0 {
		return h.heap[0].Epoch
	}
	return now.Add(time.Hour)
}

func (h *CHeap) insert(tm timeout) {
	h.heap = append(h.heap, heapItem{timeout: tm, Epoch: tm.epoch()})
	i := int32(len(h.heap)-1)
	h.heap[i].setIndex(i)
	h.up(i)
}

func (h *CHeap) remove(tm timeout) {
	i := tm.index()
	tm.setIndex(-1)
	l := int32(len(h.heap) - 1)
	if i != l {
		h.heap[i] = h.heap[l]
		h.heap[i].setIndex(i)
		h.heap = h.heap[:l]
		h.down(i)
		h.up(i)
	} else {
		h.heap = h.heap[:l]
	}
	if len(h.heap) < cap(h.heap) >> 4 {
		heap := make([]heapItem, len(h.heap), len(h.heap)*2)
		copy(heap, h.heap)
		h.heap = heap
	}
}

func (h *CHeap) pop() {
	l := int32(len(h.heap) - 1)
	h.heap[0].setIndex(-1)
	if l > 0 {
		h.heap[0] = h.heap[l]
		h.heap = h.heap[:l]
		h.down(0)
	} else {
		h.heap = h.heap[:l]
	}
	if len(h.heap) < cap(h.heap) >> 4 {
		heap := make([]heapItem, len(h.heap), len(h.heap)*2)
		copy(heap, h.heap)
		h.heap = heap
	}
}

func (h *CHeap) up(j int32) {
	item := h.heap[j]
	i := (j - 1) / 4
	if i == j || h.heap[i].Epoch < item.Epoch {
		return
	}
	h.heap[j] = h.heap[i]
	h.heap[j].setIndex(i)
	j = i

	for {
		i = (j - 1) / 4
		if i == j || h.heap[i].Epoch < item.Epoch {
			break
		}
		h.heap[j] = h.heap[i]
		h.heap[j].setIndex(j)
		j = i
	}
	h.heap[j] = item
	item.setIndex(j)
}

func (h *CHeap) down(j int32) {
	var i int32
	item := h.heap[j]
	if i = h.downIndex(j, item.Epoch); i == j {
		return
	}
	h.heap[j] = h.heap[i]
	h.heap[j].setIndex(j)
	j = i

	for {
		i = h.downIndex(j, item.Epoch)
		if i == j {
			break
		}
		h.heap[j] = h.heap[i]
		h.heap[j].setIndex(j)
		j = i
	}
	h.heap[j] = item
	item.setIndex(j)
}

func (h *CHeap) downIndex(j int32, e Epoch) int32 {
	last := int32(len(h.heap) - 1)
	if j > (last-1) / 4 {
		return j
	}
	var j1 int32
	var e1 Epoch
	i1 := j * 4 + 1
	i2 := i1+1
	if h.heap[i1].Epoch < e {
		j = i1
		e = h.heap[i1].Epoch
	}
	if i2 <= last {
		if i2+1 <= last {
			if h.heap[i2].Epoch < h.heap[i2+1].Epoch {
				j1 = i2
				e1 = h.heap[i2].Epoch
			} else {
				j1 = i2+1
				e1 = h.heap[i2+1].Epoch
			}
		} else {
			j1 = i2
			e1 = h.heap[i2].Epoch
		}
		if e1 < e {
			j = j1
			e = e1
		}
	}
	return j
}

