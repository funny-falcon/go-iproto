package net_timeout

type Kind uint8

const (
	Read = Kind(iota + 1)
	Write
)

type Action uint32

const (
	Reset = Action(iota + 1)
	Freeze
	UnFreeze
)

type state uint32

const (
	set = state(1 << iota)
	frozen
)

const (
	doSet = iota + 1
	doClear
)
