package net_timeout

import (
	"log"
	"time"
	"sync"
)

type SetDeadliner interface {
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

type Timeout struct {
	Timeout time.Duration
	mutex   sync.Mutex
	Kind    Kind
	state   state
}

func (tm *Timeout) Set(i interface{}, timeout time.Duration) {
	tm.Timeout = timeout
	tm.Reset(i)
}

func (tm *Timeout) Reset(i interface{}) {
	tm.doAction(i, reset)
}

func (tm *Timeout) Freeze(i interface{}) {
	tm.doAction(i, freeze)
}

func (tm *Timeout) UnFreeze(i interface{}) {
	tm.doAction(i, unFreeze)
}

func (tm *Timeout) doAction(i interface{}, action action) (err error) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	switch action {
	case freeze:
		tm.state &^= unfrozen
	case unFreeze:
		tm.state |= unfrozen
	case reset:
	}

	conn, ok := i.(SetDeadliner)
	if !ok {
		return
	}

	if tm.state&unfrozen != 0 && tm.Timeout > 0 {
		deadline := time.Now().Add(tm.Timeout)
		err = tm.set(conn, deadline)
	} else if tm.state&unset == 0 && (tm.state&unfrozen == 0 || tm.Timeout == 0) {
		err = tm.clear(conn)
	}
	return
}

func (tm *Timeout) set(conn SetDeadliner, deadline time.Time) (err error) {
	switch tm.Kind {
	case Read:
		err = conn.SetReadDeadline(deadline)
		tm.state &^= unset
	case Write:
		err = conn.SetWriteDeadline(deadline)
		tm.state &^= unset
	default:
		log.Panicf("Uknown Kind %d for tm", tm.Kind)
	}
	return
}

func (tm *Timeout) clear(conn SetDeadliner) (err error) {
	switch tm.Kind {
	case Read:
		err = conn.SetReadDeadline(time.Time{})
		tm.state |= unset
	case Write:
		err = conn.SetWriteDeadline(time.Time{})
		tm.state |= unset
	default:
		log.Panicf("Uknown Kind %d for tm", tm.Kind)
	}
	return
}
