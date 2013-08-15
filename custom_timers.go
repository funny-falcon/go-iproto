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
	heap, send, recv int32
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

	d.heap = int32(int(heapsI.Incr()) % len(heaps))
	heaps[d.heap].Insert(sendTimeout{d})
	heaps[d.heap].Insert(recvTimeout{d})
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
	heaps[d.heap].Remove(sendTimeout{d})
	heaps[d.heap].Remove(recvTimeout{d})
	prev := d.basic.Unchain()
	if prev != nil {
		prev.Respond(res)
	}
}

func (d *CDeadline) Cancel() {
	heaps[d.heap].Remove(sendTimeout{d})
	heaps[d.heap].Remove(recvTimeout{d})
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
	heap [][]heapItem
	size int32
	changed chan bool
}

func (h *CHeap) Init() {
	h.heap = make([][]heapItem, 1)
	h.heap[0] = make([]heapItem, 256)
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
	for h.size > 0 {
		t := h.heap[0][0]
		if t.Sub(now) >= 0 {
			break
		}
		h.pop()
		h.Unlock()
		t.expire()
		h.Lock()
	}
	if h.size > 0 {
		return h.heap[0][0].Epoch
	}
	return now.Add(time.Hour)
}

func (h *CHeap) insert(tm timeout) {
	if h.size & 0xff == 0 {
		h.heap = append(h.heap, make([]heapItem, 256))
	}
	h.heap[h.size>>8][h.size&0xff] = heapItem{timeout: tm, Epoch: tm.epoch()}
	i := h.size
	tm.setIndex(i)
	h.size++
	h.up(i)
}

func (h *CHeap) get(i int32) heapItem {
	return h.heap[i>>8][i&0xff]
}

func (h *CHeap) getEpoch(i int32) Epoch {
	return h.heap[i>>8][i&0xff].Epoch
}

func (h *CHeap) set(i int32, item heapItem) {
	h.heap[i>>8][i&0xff] = item
	item.setIndex(i)
}

func (h *CHeap) move(from, to int32) {
	item := h.heap[from>>8][from&0xff]
	h.heap[to>>8][to&0xff] = item
	item.setIndex(to)
}

func (h *CHeap) remove(tm timeout) {
	i := tm.index()
	tm.setIndex(-1)
	l := h.size - 1
	if i != l {
		h.move(l, i)
		h.size--
		h.down(i)
		h.up(i)
	} else {
		h.size--
	}
	if h.size>>8 > 0 && h.size>>8 < int32(len(h.heap)) {
		h.heap[len(h.heap)] = nil
		h.heap = h.heap[:len(h.heap)-1]
	} else {
		h.set(h.size, heapItem{})
	}
}

func (h *CHeap) pop() {
	l := h.size - 1
	h.heap[0][0].setIndex(-1)
	if l > 0 {
		h.move(l, 0)
		h.size--
		h.down(0)
	} else {
		h.size--
	}
	if h.size>>8 > 0 && h.size>>8 < int32(len(h.heap)) {
		h.heap[len(h.heap)] = nil
		h.heap = h.heap[:len(h.heap)-1]
	} else {
		h.set(h.size, heapItem{})
	}
}

func (h *CHeap) up(j int32) {
	item := h.get(j)
	i := (j - 1) / 4
	if i == j || h.getEpoch(i) < item.Epoch {
		return
	}
	h.move(i, j)
	j = i

	for {
		i = (j - 1) / 4
		if i == j || h.getEpoch(i) < item.Epoch {
			break
		}
		h.move(i, j)
		j = i
	}
	h.set(j, item)
	item.setIndex(j)
}

func (h *CHeap) down(j int32) {
	var i int32
	item := h.get(j)
	if i = h.downIndex(j, item.Epoch); i == j {
		return
	}
	h.move(i, j)
	j = i

	for {
		i = h.downIndex(j, item.Epoch)
		if i == j {
			break
		}
		h.move(i, j)
		j = i
	}
	h.set(j, item)
	item.setIndex(j)
}

func (h *CHeap) downIndex(j int32, e Epoch) int32 {
	last := h.size - 1
	if j > (last-1) / 4 {
		return j
	}
	var j2 int32
	var e1, e2 Epoch
	i1 := j * 4 + 1
	i2 := i1+1
	
	e1 = h.getEpoch(i1)
	if e1 < e {
		j = i1
		e = e1
	}
	if i2 <= last {
		if i2+1 <= last {
			e21, e22 := h.getEpoch(i2), h.getEpoch(i2+1)
			if e21 < e22 {
				j2 = i2
				e2 = e21
			} else {
				j2 = i2+1
				e2 = e22
			}
		} else {
			j2 = i2
			e2 = h.getEpoch(i2)
		}
		if e2 < e {
			j = j2
			e = e2
		}
	}
	return j
}

