package util

import (
	"sync"
	"sync/atomic"
)

type Atomic uint32

func (a *Atomic) CAS(old Atomic, set Atomic) bool {
	return atomic.CompareAndSwapUint32((*uint32)(a), uint32(old), uint32(set))
}

func (r *Atomic) Store(st Atomic) {
	atomic.StoreUint32((*uint32)(r), uint32(st))
}

func (r *Atomic) Is(st Atomic) bool {
	return atomic.LoadUint32((*uint32)(r)) == uint32(st)
}

func (r *Atomic) Get() Atomic {
	return Atomic(atomic.LoadUint32((*uint32)(r)))
}

func (r *Atomic) Incr() Atomic {
	return Atomic(atomic.AddUint32((*uint32)(r), 1))
}

func (r *Atomic) Decr() Atomic {
	return Atomic(atomic.AddUint32((*uint32)(r), ^uint32(0)))
}

type Synced struct {
	sync.Mutex
	State uint32
}

func (a *Synced) CAS(old uint32, set uint32) bool {
	if a.State == old {
		a.State = set
		return true
	}
	return false
}

func (a *Synced) Store(st uint32) {
	a.State = st
}

func (a *Synced) Is(st uint32) bool {
	return a.State == st
}

func (a *Synced) Get() uint32 {
	return a.State
}
