package sbox

import (
	"github.com/funny-falcon/go-iproto"
	"github.com/funny-falcon/go-iproto/marshal"
	"log"
	"reflect"
)

var _ = log.Printf

type InsertMode uint16

const (
	InsertOrReplace = InsertMode(0)
	Insert          = InsertMode(1)
	Replace         = InsertMode(2)
)

type StoreReq struct {
	Space  uint32
	Return bool
	Mode   uint16
	Tuple  interface{}
}

func (i StoreReq) IWrite(w *marshal.Writer) {
	w.Uint32(i.Space)
	var flags uint32
	if i.Return {
		flags = 1
	}
	flags |= uint32(i.Mode) << 1
	w.Uint32(flags)
	WriteTuple(w, i.Tuple)
}

func (i StoreReq) IMsg() iproto.RequestType {
	return 13
}

type DeleteReq struct {
	Space  uint32
	Return bool
	Key    interface{}
}

func (i DeleteReq) IWrite(w *marshal.Writer) {
	w.Uint32(i.Space)
	var flags uint32
	if i.Return {
		flags = 1
	}
	w.Uint32(flags)
	WriteTuple(w, i.Key)
}

func (i DeleteReq) IMsg() iproto.RequestType {
	return 21
}

type UpdateReq struct {
	Space  uint32
	Return bool
	Key    interface{}
	Ops    []Op
}

func (u UpdateReq) IWrite(w *marshal.Writer) {
	w.Uint32(u.Space)
	var flags uint32
	if u.Return {
		flags = 1
	}
	w.Uint32(flags)
	WriteTuple(w, u.Key)
	w.Write(u.Ops)
}

func (i UpdateReq) IMsg() iproto.RequestType {
	return 19
}

type OpKind byte

const (
	OpSet    = OpKind('=')
	OpAdd    = OpKind('+')
	OpAnd    = OpKind('&')
	OpOr     = OpKind('|')
	OpXor    = OpKind('^')
	OpSplice = OpKind('s')
	OpDelete = OpKind('d')
	OpInsert = OpKind('i')
)

var opmap = [...]byte{
	OpSet:    0,
	OpAdd:    1,
	OpAnd:    2,
	OpOr:     3,
	OpXor:    4,
	OpSplice: 5,
	OpDelete: 6,
	OpInsert: 7,
}

type Op struct {
	Field uint32
	Op    OpKind
	Val   interface{} `iproto:size(ber)`
}

func (o Op) IWrite(w *marshal.Writer) {
	w.Write(o.Field)
	if o.Op <= 7 {
		w.Write(o.Op)
	} else {
		w.Write(opmap[o.Op])
	}
	switch v := o.Val.(type) {
	case uint32:
		w.Int8(4)
		w.Uint32(v)
	case uint64:
		w.Int8(8)
		w.Uint64(v)
	case int32:
		w.Int8(4)
		w.Int32(v)
	case int64:
		w.Int8(8)
		w.Int64(v)
	case []byte:
		w.Intvar(len(v))
		w.Bytes(v)
	case string:
		w.Intvar(len(v))
		w.String(v)
	default:
		val := reflect.ValueOf(v)
		wr := marshal.WriterFor(val.Type())
		wr.WithSize(w, val, (*marshal.Writer).Intvar)
	}
}

type Slice struct {
	Offset int32       `iproto:size(ber)`
	Length int32       `iproto:size(ber)`
	Val    interface{} `iproto:size(ber)`
}
