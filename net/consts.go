package net

type Kind uint8

const (
	Read = Kind(iota + 1)
	Write
)

type RCType uint32

const (
	RC4byte = RCType(iota)
	RC1byte
	RC0byte
)
