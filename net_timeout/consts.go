package net_timeout

type Kind uint8

const (
	Read = Kind(iota + 1)
	Write
)

type action uint32

const (
	reset = action(iota + 1)
	freeze
	unFreeze
)

type state uint32

const (
	unset = state(1 << iota)
	unfrozen
)

const (
	doSet = iota + 1
	doClear
)
