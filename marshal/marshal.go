package marshal

import (
	"encoding/binary"
	"reflect"
	"sync/atomic"
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

var readerLock uint32 = 0
var readerCache unsafe.Pointer = unsafe.Pointer(&Reader{})

func Read(b []byte, i interface{}) (err error) {
	var t unsafe.Pointer
	var r *Reader
	if t = readerCache; t != nil {
		if atomic.CompareAndSwapPointer(&readerCache, t, nil) {
			r = (*Reader)(t)
			goto Got
		}
	}
	r = &Reader{Body: b}
Got:
	r.Body = b
	r.Err = nil
	err = r.Read(i)
	r.Body = nil
	atomic.StorePointer(&readerCache, unsafe.Pointer(r))
	return
}

func ReadTail(b []byte, i interface{}) (err error) {
	var t unsafe.Pointer
	var r *Reader
	if t = readerCache; t != nil {
		if atomic.CompareAndSwapPointer(&readerCache, t, nil) {
			r = (*Reader)(t)
			goto Got
		}
	}
	r = &Reader{Body: b}
Got:
	r.Body = b
	r.Err = nil
	err = r.ReadTail(i)
	r.Body = nil
	atomic.StorePointer(&readerCache, unsafe.Pointer(r))
	return
}

var writerLock uint32 = 0
var writerCache Writer

func Write(i interface{}) (res []byte) {
	if atomic.CompareAndSwapUint32(&writerLock, 0, 1) {
		writerCache.Write(i)
		res = writerCache.Written()
		atomic.StoreUint32(&writerLock, 0)
	} else {
		w := Writer{DefSize: 32}
		w.Write(i)
		res = w.Written()
	}
	return
}

func WriteTail(i interface{}) (res []byte) {
	if atomic.CompareAndSwapUint32(&readerLock, 0, 1) {
		writerCache.WriteTail(i)
		res = writerCache.Written()
		atomic.StoreUint32(&writerLock, 0)
	} else {
		w := Writer{DefSize: 32}
		w.WriteTail(i)
		res = w.Written()
	}
	return
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
