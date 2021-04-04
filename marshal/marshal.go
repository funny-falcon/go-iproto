package marshal

import (
	"encoding/binary"
	"reflect"
	"sync"
	"unsafe"
)

var le = binary.LittleEndian

type IWriter interface {
	IWrite(self interface{}, w *Writer)
}

type IShortWriter interface {
	IWrite(w *Writer)
}

type ISizer interface {
	ISize(self interface{}) int
}

type IShortSizer interface {
	ISize() int
}

type ICounter interface {
	ICount(self interface{}) int
}

type IShortCounter interface {
	ICount() int
}

type IReader interface {
	IRead(self interface{}, r *Reader)
}

type IShortReader interface {
	IRead(r *Reader)
}

type IReadTailer interface {
	IReadTail(self interface{}, r *Reader)
}

type IShortReadTailer interface {
	IReadTail(r *Reader)
}

type ISetSizer interface {
	ISetSize(self interface{}, len int) (bool, error)
}

type IShortSetSizer interface {
	ISetSize(int) (bool, error)
}

type ISetCounter interface {
	ISetCount(self interface{}, len int) error
}

type IShortSetCounter interface {
	ISetCount(int) error
}

//var readerLock uint32 = 0
var readerCache unsafe.Pointer = unsafe.Pointer(&Reader{})
var readerPool = sync.Pool{
	New: func() interface{} {
		return &Reader{}
	},
}

func Read(b []byte, i interface{}) (err error) {
	r := readerPool.Get().(*Reader)
	r.Body = b
	err = r.Read(i)
	*r = Reader{}
	readerPool.Put(r)
	return err
}

func ReadTail(b []byte, i interface{}) (err error) {
	r := readerPool.Get().(*Reader)
	r.Body = b
	err = r.ReadTail(i)
	*r = Reader{}
	readerPool.Put(r)
	return err
}

var writerPool = sync.Pool{
	New: func() interface{} {
		return &Writer{DefSize: 512}
	},
}

func Write(i interface{}) (res []byte) {
	w := writerPool.Get().(*Writer)
	w.Write(i)
	res = w.Written()
	writerPool.Put(w)
	return res
}

func WriteTail(i interface{}) (res []byte) {
	w := writerPool.Get().(*Writer)
	w.WriteTail(i)
	res = w.Written()
	writerPool.Put(w)
	return res
}

type efaceHeader struct {
	c, v uintptr
}

func classof(o interface{}) uintptr {
	return (*efaceHeader)(unsafe.Pointer(&o)).c
}

var iinterface = reflect.TypeOf(new(interface{})).Elem()

var iwriter = reflect.TypeOf(new(IWriter)).Elem()
var ishortwriter = reflect.TypeOf(new(IShortWriter)).Elem()
var isizer = reflect.TypeOf(new(ISizer)).Elem()
var ishortsizer = reflect.TypeOf(new(IShortSizer)).Elem()
var icounter = reflect.TypeOf(new(ICounter)).Elem()
var ishortcounter = reflect.TypeOf(new(IShortCounter)).Elem()

var ireader = reflect.TypeOf(new(IReader)).Elem()
var ishortreader = reflect.TypeOf(new(IShortReader)).Elem()
var ireadtailer = reflect.TypeOf(new(IReadTailer)).Elem()
var ishortreadtailer = reflect.TypeOf(new(IShortReadTailer)).Elem()
var isetsizer = reflect.TypeOf(new(ISetSizer)).Elem()
var ishortsetsizer = reflect.TypeOf(new(IShortSetSizer)).Elem()
var isetcounter = reflect.TypeOf(new(ISetCounter)).Elem()
var ishortsetcounter = reflect.TypeOf(new(IShortSetCounter)).Elem()

var tint8 = reflect.TypeOf(int8(0))
var tint16 = reflect.TypeOf(int16(0))
var tint32 = reflect.TypeOf(int32(0))
var tint64 = reflect.TypeOf(int64(0))
var tuint8 = reflect.TypeOf(uint8(0))
var tuint16 = reflect.TypeOf(uint16(0))
var tuint32 = reflect.TypeOf(uint32(0))
var tuint64 = reflect.TypeOf(uint64(0))
var tfloat32 = reflect.TypeOf(float32(0))
var tfloat64 = reflect.TypeOf(float64(0))

const gg = 2*1024*1024*1024 - 1

func Varsize(i int) (j int) {
	for j = 0; i > 1<<7; j++ {
		i >>= 7
	}
	return j + 1
}
