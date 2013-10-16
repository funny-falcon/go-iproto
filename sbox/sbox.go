package sbox

type TailType int

const (
	NoTail = TailType(iota)
	Tail
	TailSplit
)
