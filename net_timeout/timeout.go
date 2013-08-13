package net_timeout

import (
	"log"
	"time"
)

type SetDeadliner interface {
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

type Timeout struct {
	Timeout time.Duration
	state   state
	action  Action
	Actions chan Action
}

func (tm *Timeout) Init() {
	tm.state = frozen
	tm.Actions = make(chan Action, 2)
}

func (tm *Timeout) PingAction(action Action) {
	curAction := tm.action
	if curAction == action {
		return
	}
	if curAction == Freeze && action != UnFreeze {
		return
	}
	tm.action = action
	tm.Actions <- action
}

func (tm *Timeout) DoAction(i interface{}, kind Kind, action Action) (err error) {
	conn, ok := i.(SetDeadliner)
	if !ok {
		return
	}

	switch action {
	case Freeze:
		tm.state |= frozen
	case UnFreeze:
		tm.state &^= frozen
	case Reset:
	}

	if tm.state&frozen == 0 && tm.Timeout > 0 {
		deadline := time.Now().Add(tm.Timeout)
		err = tm.set(conn, kind, deadline)
	} else if tm.state&set != 0 && (tm.state&frozen != 0 || tm.Timeout == 0) {
		err = tm.clear(conn, kind)
	}
	return
}

func (tm *Timeout) clear(conn SetDeadliner, kind Kind) (err error) {
	switch kind {
	case Read:
		err = conn.SetReadDeadline(time.Time{})
		tm.state &^= set
	case Write:
		err = conn.SetWriteDeadline(time.Time{})
		tm.state &^= set
	default:
		log.Panicf("Uknown Kind %d for tm", kind)
	}
	return
}

func (tm *Timeout) set(conn SetDeadliner, kind Kind, deadline time.Time) (err error) {
	switch kind {
	case Read:
		err = conn.SetReadDeadline(deadline)
		tm.state |= set
	case Write:
		err = conn.SetWriteDeadline(deadline)
		tm.state |= set
	default:
		log.Panicf("Uknown Kind %d for tm", kind)
	}
	return
}
