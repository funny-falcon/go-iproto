package sbox

import (
	"log"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/funny-falcon/go-iproto/marshal"
)

var readerLock uint32 = 0
var readerCache unsafe.Pointer = unsafe.Pointer(&[2]marshal.Reader{})

func ReadFirst(b []byte, v interface{}) (read bool, total int, err error) {
	var t unsafe.Pointer
	var r *[2]marshal.Reader
	if t = readerCache; t != nil {
		if atomic.CompareAndSwapPointer(&readerCache, t, nil) {
			r = (*[2]marshal.Reader)(t)
			*r = [2]marshal.Reader{{Body: b}, {}}
			goto Got
		}
	}
	r = &[2]marshal.Reader{{Body: b}, {}}
Got:
	total = r[0].IntUint32()
	if total > 0 {
		sz := r[0].IntUint32()
		if r[0].Err == nil {
			r[1].Body = r[0].Slice(sz + 4)
			err = ReadRawTuple(&r[1], v)
			read = true
		} else {
			err = r[0].Err
		}
	}
	atomic.StorePointer(&readerCache, unsafe.Pointer(r))
	return
}

func ReadMany(b []byte, v interface{}) (read, total int, err error) {
	var t unsafe.Pointer
	var r *[2]marshal.Reader
	if t = readerCache; t != nil {
		if atomic.CompareAndSwapPointer(&readerCache, t, nil) {
			r = (*[2]marshal.Reader)(t)
			*r = [2]marshal.Reader{{Body: b}, {}}
			goto Got
		}
	}
	r = &[2]marshal.Reader{{Body: b}, {}}
Got:
	total = r[0].IntUint32()

	read = readInterface(r, reflect.ValueOf(v), total)

	err = r[0].Err
	atomic.StorePointer(&readerCache, unsafe.Pointer(r))
	return
}

func readInterface(r *[2]marshal.Reader, v reflect.Value, l int) int {
	switch v.Kind() {
	case reflect.Ptr:
		el := v.Elem()
		switch el.Kind() {
		case reflect.Array:
			return readArray(r, el, l)
		case reflect.Slice:
			return readSlice(r, el, l)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Float32, reflect.Float64, reflect.Struct, reflect.String, reflect.Ptr:
			oneTuple(r, el, nil)
			return 1
		}
	case reflect.Array, reflect.Slice:
		return readFixedArray(r, v, l)
	}
	log.Panicf("Don't know how to read tuple into", v.Type())
	return 0
}

func readFixedArray(r *[2]marshal.Reader, v reflect.Value, l int) int {
	tel := v.Type().Elem()
	switch tel.Kind() {
	case reflect.Slice, reflect.Ptr:
		rd := reader(v.Type())
		ln := v.Len()
		var i int
		for i = 0; i < l && i < ln; i++ {
			if oneTuple(r, v.Index(i), rd) != nil {
				break
			}
		}
		return i
	case reflect.Interface:
		return readInterfaceArray(r, v, l)
	default:
		log.Panicf("Do not know how to read tuple into %+v", tel)
	}
	return 0
}

func readArray(r *[2]marshal.Reader, v reflect.Value, l int) int {
	tel := v.Type().Elem()
	switch tel.Kind() {
	case reflect.Array, reflect.Slice:
		switch tel.Elem().Kind() {
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Float32, reflect.Float64, reflect.String:
		default:
			log.Panicf("Do not know how to read tuple into %+v", tel)
		}
		fallthrough
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64, reflect.Struct, reflect.String, reflect.Ptr:
		rd := reader(tel)
		ln := v.Len()
		var i int
		for ; i < l && i < ln; i++ {
			if oneTuple(r, v.Index(i), rd) != nil {
				break
			}
		}
		return i
	case reflect.Interface:
		return readInterfaceArray(r, v, l)
	default:
		log.Panicf("Do not know how to read tuple into %+v", tel)
	}
	return 0
}

func readSlice(r *[2]marshal.Reader, v reflect.Value, l int) int {
	tel := v.Type().Elem()
	switch tel.Kind() {
	case reflect.Array, reflect.Slice:
		switch tel.Elem().Kind() {
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Float32, reflect.Float64, reflect.String:
		default:
			log.Panicf("Do not know how to read tuple into %+v", tel)
		}
		fallthrough
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64, reflect.Struct, reflect.String, reflect.Ptr:
		rd := reader(tel)
		v.Set(reflect.MakeSlice(v.Type(), l, l))
		var i int
		for ; i < l; i++ {
			if oneTuple(r, v.Index(i), rd) != nil {
				break
			}
		}
		return i
	case reflect.Interface:
		return readInterfaceArray(r, v, l)
	default:
		log.Panicf("Do not know how to read tuple into %+v", tel)
	}
	return 0
}

func readInterfaceArray(r *[2]marshal.Reader, v reflect.Value, l int) int {
	var i int
	ln := v.Len()
	for i = 0; i < ln && l > 0; i++ {
		l -= readInterface(r, v.Index(i), l)
		if r[0].Err != nil {
			break
		}
	}
	return i
}

func oneTuple(r *[2]marshal.Reader, v reflect.Value, rd *TReader) error {
	if r[0].Err != nil {
		return r[0].Err
	}
	sz := r[0].IntUint32()
	r[1].Body = r[0].Slice(sz + 4)
	if r[0].Err != nil {
		log.Printf("oneTuple header read error: %s", r[0].Err)
		return r[0].Err
	}
	body := r[1].Body
	if rd == nil {
		rd = reader(v.Type())
	}
	rd.Auto(&r[1], v)
	if r[1].Err != nil {
		log.Printf("oneTuple read error: %s %+v [% x]", r[1].Err, v.Interface(), body)
	}
	//r[0].Err = r[1].Err
	//return r[0].Err
	return nil
}
