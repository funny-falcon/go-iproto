package req_initer

import (
	algo "container/heap"
)

type heapKind uint32

const (
	_send = heapKind(iota + 1)
	_recv
)

type heap struct {
	heap []*Request
	kind heapKind
}

//implements Sort.Len
func (h *heap) Len() int {
	return len(h.heap)
}

//implements Sort.Less
func (h *heap) Less(i, j int) bool {
	return h.heap[i].before(h.heap[j], h.kind)
}

//implements Sort.Swap
func (h *heap) Swap(i, j int) {
	l, r := h.heap[i], h.heap[j]
	h.heap[i], h.heap[j] = r, l
	h.heap[i].setIndex(i, h.kind)
	h.heap[j].setIndex(j, h.kind)
}

//implements Heap.Push
func (h *heap) Push(r interface{}) {
	request := r.(*Request)
	n := len(h.heap)
	h.heap = append(h.heap, request)
	h.heap[n].setIndex(n, h.kind)
}

//implements Heap.Pop
func (h *heap) Pop() interface{} {
	last := len(h.heap) - 1
	h.heap[last].setIndex(-1, h.kind)
	res := h.heap[last]
	h.heap = h.heap[0:last]
	return res
}

func (h *heap) Add(request *Request) {
	algo.Push(h, request)
}

func (h *heap) First() *Request {
	if len(h.heap) > 0 {
		return h.heap[0]
	} else {
		return nil
	}
}

func (h *heap) Shift() *Request {
	return algo.Pop(h).(*Request)
}

func (h *heap) Remove(request *Request) {
	if index := request.index(h.kind); index >= 0 {
		algo.Remove(h, index)
	}
}
