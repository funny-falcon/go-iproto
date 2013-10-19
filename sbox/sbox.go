package sbox

import (
	"github.com/funny-falcon/go-iproto"
)

type TailType int

const (
	NoTail = TailType(iota)
	Tail
	TailSplit
)

const (
	RcOK                   = iproto.RcOK
	RcReadOnly             = iproto.RetCode(0x0401)
	RcLocked               = iproto.RetCode(0x0601)
	RcMemoryIssue          = iproto.RetCode(0x0701)
	RcNonMaster            = iproto.RetCode(0x0102)
	RcIllegalParams        = iproto.RetCode(0x0202)
	RcSecondaryPort        = iproto.RetCode(0x0301)
	RcBadIntegrity         = iproto.RetCode(0x0801)
	RcUnsupportedCommand   = iproto.RetCode(0x0a02)
	RcDuplicate            = iproto.RetCode(0x2002)
	RcWrongField           = iproto.RetCode(0x1e02)
	RcWrongNumber          = iproto.RetCode(0x1f02)
	RcWrongVersion         = iproto.RetCode(0x2602)
	RcWalIO                = iproto.RetCode(0x2702)
	RcDoesntExists         = iproto.RetCode(0x3102)
	RcStoredProcNotDefined = iproto.RetCode(0x3202)
	RcLuaError             = iproto.RetCode(0x3302)
	RcTupleExists          = iproto.RetCode(0x3702)
	RcDuplicateKey         = iproto.RetCode(0x3802)
)
