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
	heap, send, recv uint32
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

	d.heap = uint32(int(heapsI.Incr()) % len(heaps))
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
	index() int
	setIndex(int)
	expire()
}

type sendTimeout struct {
	*CDeadline
}

func (s sendTimeout) epoch() Epoch {
	d := s.CDeadline
	req := d.basic.Request
	return req.Deadline - Epoch(req.WorkTime)
}
func (s sendTimeout) index() int {
	d := s.CDeadline
	return int(d.send)
}
func (s sendTimeout) setIndex(i int) {
	d := s.CDeadline
	d.send = uint32(i)
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
	return req.Deadline
}
func (s recvTimeout) index() int {
	d := s.CDeadline
	return int(d.recv)
}
func (s recvTimeout) setIndex(i int) {
	d := s.CDeadline
	d.recv = uint32(i)
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
	heap []*[256]heapItem
	size int
	changed chan bool
}

func (h *CHeap) Init() {
	h.heap = make([]*[256]heapItem, 1)
	h.heap[0] = &[256]heapItem{}
	h.changed = make(chan bool, 1)
	h.size = 3 // fake start index for compacter items layout
	go h.Loop()
}

func (h *CHeap) Insert(t timeout) {
	if t.index() != 0 {
		panic("timeout insert: tm.index() != 0")
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
	if t.index() == 0 {
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
	for h.size > 3 {
		t := h.heap[0][3]
		if t.Sub(now) >= 0 {
			break
		}
		h.pop()
		h.Unlock()
		t.expire()
		h.Lock()
	}
	if h.size > 3 {
		return h.heap[0][3].Epoch
	}
	return now.Add(time.Hour)
}

func (h *CHeap) insert(tm timeout) {
	if h.size & 0xff == 0 && h.size >> 8 == len(h.heap) {
		h.heap = append(h.heap, &[256]heapItem{})
	}
	h.heap[h.size>>8][h.size&0xff] = heapItem{timeout: tm, Epoch: tm.epoch()}
	i := h.size
	tm.setIndex(i)
	h.size++
	h.up(i)
}

func (h *CHeap) get(i int) heapItem {
	return h.heap[i>>8][i&0xff]
}

func (h *CHeap) getEpoch(i int) Epoch {
	return h.heap[i>>8][i&0xff].Epoch
}

func (h *CHeap) clear(i int) {
	h.heap[i>>8][i&0xff] = heapItem{}
}

func (h *CHeap) set(i int, item heapItem) {
	h.heap[i>>8][i&0xff] = item
	if item.timeout != nil {
		item.setIndex(i)
	}
}

func (h *CHeap) move(from, to int) {
	item := h.heap[from>>8][from&0xff]
	h.heap[to>>8][to&0xff] = item
	item.setIndex(to)
}

func (h *CHeap) chomp() {
	chunks := ((h.size-1) >> 8) + 1
	if chunks + 1 < len(h.heap) {
		h.heap[len(h.heap)-1] = nil
		h.heap = h.heap[:chunks+1]
	} else {
		h.heap[h.size>>8][h.size&0xff] = heapItem{}
	}
}

func (h *CHeap) remove(tm timeout) {
	i := tm.index()
	tm.setIndex(0)
	h.size--
	l := h.size
	if i != l {
		h.move(l, i)
		h.down(i)
		h.up(i)
	}
	h.chomp()
}

func (h *CHeap) pop() {
	h.size--
	l := h.size
	h.heap[0][3].setIndex(0)
	if l > 3 {
		h.move(l, 3)
		h.down(3)
	} else {
		h.size--
	}
	h.chomp()
}

func (h *CHeap) up(j int) {
	item := h.get(j)
	i := j / 4 + 2
	if i == 2 || h.getEpoch(i) < item.Epoch {
		return
	}
	h.move(i, j)
	j = i

	for {
		i = j / 4 + 2
		if i == j || h.getEpoch(i) < item.Epoch {
			break
		}
		h.move(i, j)
		j = i
	}
	h.set(j, item)
	item.setIndex(j)
}

func (h *CHeap) down(j int) {
	var i int

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

func (h *CHeap) downIndex(j int, e Epoch) int {
	last := h.size - 1
	if j > last / 4 + 2 {
		return j
	}
	var j2 int
	var e1, e2 Epoch

	i1 := (j - 2) * 4
	i2 := i1 + 1

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
