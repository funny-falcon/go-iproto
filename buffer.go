package iproto

import (
	"log"
	"sync"
	"sync/atomic"
)

var _ = log.Print

type bufBookmark struct {
	Bookmark
	state uint32
}

func (b *bufBookmark) Respond(r *Response) {
	b.state = bsFree
}

const (
	bsNew = iota
	bsSet
	bsFree
)

const (
	bufRow = 1024
)

type bufferRow struct {
	id  uint64
	row [bufRow]bufBookmark
}

type Buffer struct {
	ch         chan *Request
	onExit     func()
	set        chan bool
	m          sync.Mutex
	rows       map[uint64]*bufferRow
	head, tail uint64
	hRow, tRow *bufferRow
}

func (b *Buffer) init() {
	b.rows = make(map[uint64]*bufferRow)
	row := new(bufferRow)
	b.hRow, b.tRow, b.rows[0] = row, row, row
	b.set = make(chan bool, 1)
}

func (b *Buffer) push(r *Request) {
	if b.tail == b.head {
		select {
		case b.ch <- r:
			return
		default:
		}
	}

	tail := atomic.AddUint64(&b.tail, 1) - 1
	big := tail / bufRow
	row := b.tRow
	if row.id != big {
		var ok bool
		b.m.Lock()
		if row, ok = b.rows[big]; !ok {
			row = &bufferRow{id: big}
			b.rows[big] = row
			b.tRow = row
		}
		b.m.Unlock()
	}

	middle := &row.row[tail%bufRow]
	if r.ChainBookmark(middle) {
		middle.state = bsSet
	} else {
		middle.state = bsFree
	}
	select {
	case b.set <- true:
	default:
	}
}

func (b *Buffer) close() {
	select {
	case b.set <- true:
	default:
	}
	close(b.set)
}

func (b *Buffer) loop() {
	for <-b.set {
	Tiny:
		for ; b.head < atomic.LoadUint64(&b.tail); b.head++ {
			var ok bool
			big := b.head / bufRow
			row := b.hRow
			if row.id != big {
				b.m.Lock()
				row, ok = b.rows[big]
				delete(b.rows, big-1)
				b.m.Unlock()
				if !ok {
					break
				}
				b.hRow = row
			}
			middle := &row.row[b.head%bufRow]
			switch middle.state {
			case bsNew:
				break Tiny
			case bsSet:
				req := middle.Request
				middle.state = bsFree
				if req != nil && req.IsPending() {
					b.ch <- req
				}
			case bsFree:
			}
		}
	}
	if b.onExit != nil {
		b.onExit()
	}
}
