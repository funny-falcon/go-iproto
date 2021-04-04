package marshal

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"unsafe"
)

var _ = log.Print

type Reader struct {
	Body []byte
	Err  error
}

func (r *Reader) Uint8() (res uint8) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < 1 {
		r.Err = errors.New("iproto.Reader: not enough data for uint8")
		return
	}
	res = r.Body[0]
	r.Body = r.Body[1:]
	return
}

func (r *Reader) Int8() (res int8) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < 1 {
		r.Err = errors.New("iproto.Reader: not enough data for int8")
		return
	}
	res = int8(r.Body[0])
	r.Body = r.Body[1:]
	return
}

const maxUint64 = 1<<64 - 1

func (r *Reader) Uint64var() (res uint64) {
	if r.Err != nil {
		return
	}
	l := len(r.Body)
	var i int
	for i = 0; i < l; i++ {
		res = (res << 7) | uint64(r.Body[i]&0x7f)
		if r.Body[i] < 0x80 {
			r.Body = r.Body[i+1:]
			return
		} else if res > (maxUint64 >> 7) {
			r.Err = fmt.Errorf("iproto.Reader: varint is too big %x", r)
			return
		}
	}
	r.Err = fmt.Errorf("iproto.Reader: not enough data for uint64var %x", r)
	return
}

func (r *Reader) Intvar() (res int) {
	i := r.Uint64var()
	return int(i)
}

func (r *Reader) IntUint8() (res int) {
	i := r.Uint8()
	return int(i)
}

func (r *Reader) IntUint16() (res int) {
	i := r.Uint16()
	return int(i)
}

func (r *Reader) IntUint32() (res int) {
	i := r.Uint32()
	return int(i)
}

func (r *Reader) IntUint64() (res int) {
	i := r.Uint64()
	return int(i)
}

func (r *Reader) Uint8sl(b []uint8) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < len(b) {
		r.Err = errors.New("iproto.Reader: not enough data for []uint8")
		return
	}
	copy(b, r.Body)
	r.Body = r.Body[len(b):]
	return
}

func (r *Reader) Bytes(b []byte) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < len(b) {
		r.Err = errors.New("iproto.Reader: not enough data for []byte")
		return
	}
	copy(b, r.Body)
	r.Body = r.Body[len(b):]
	return
}

func (r *Reader) Int8sl(b []int8) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < len(b) {
		r.Err = errors.New("iproto.Reader: not enough data for []uint8")
		return
	}
	for i := 0; i < len(b); i++ {
		b[i] = int8(r.Body[i])
	}
	r.Body = r.Body[len(b):]
	return
}

func (r *Reader) Slice(sz int) (res []byte) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < sz {
		r.Err = fmt.Errorf("iproto.Reader: not enough data for Slice(%d)", sz)
		return
	}
	res = r.Body[:sz]
	r.Body = r.Body[sz:]
	return
}

func (r *Reader) String(sz int) (res string) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < sz {
		r.Err = fmt.Errorf("iproto.Reader: not enough data for Slice(%d)", sz)
		return
	}
	res = string(r.Body[:sz])
	r.Body = r.Body[sz:]
	return
}

func (r *Reader) Tail() (res []byte) {
	if r.Err != nil {
		return
	}
	res = r.Body
	r.Body = nil
	return
}

func (r *Reader) Uint8Val(v reflect.Value) {
	v.SetUint(uint64(r.Uint8()))
}

func (r *Reader) Uint16Val(v reflect.Value) {
	v.SetUint(uint64(r.Uint16()))
}

func (r *Reader) Uint32Val(v reflect.Value) {
	v.SetUint(uint64(r.Uint32()))
}

func (r *Reader) Uint64Val(v reflect.Value) {
	v.SetUint(r.Uint64())
}

func (r *Reader) Int8Val(v reflect.Value) {
	v.SetInt(int64(r.Int8()))
}

func (r *Reader) Int16Val(v reflect.Value) {
	v.SetInt(int64(r.Int16()))
}

func (r *Reader) Int32Val(v reflect.Value) {
	v.SetInt(int64(r.Int32()))
}

func (r *Reader) Int64Val(v reflect.Value) {
	v.SetInt(r.Int64())
}

func (r *Reader) Float32Val(v reflect.Value) {
	v.SetFloat(float64(r.Float32()))
}

func (r *Reader) Float64Val(v reflect.Value) {
	v.SetFloat(r.Float64())
}

func (r *Reader) Uint64varVal(v reflect.Value) {
	v.SetUint(r.Uint64var())
}

func (r *Reader) Uint8slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		el0 := v.Index(0)
		if el0.Kind() != reflect.Uint8 {
			panic("Uint8slVal called on wrong slice")
		}
		sh := sliceHeaderFromElem(el0, l)
		p := *(*[]byte)(unsafe.Pointer(&sh))
		r.Uint8sl(p[:l])
	}
}

func (r *Reader) Int8slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		el0 := v.Index(0)
		if el0.Kind() != reflect.Int8 {
			panic("Int8slVal called on wrong slice")
		}
		sh := sliceHeaderFromElem(el0, l)
		p := *(*[]byte)(unsafe.Pointer(&sh))
		r.Uint8sl(p)
	}
}

func (r *Reader) Uint8slValTail(v reflect.Value) {
	if r.Err != nil {
		return
	}
	if v.CanAddr() {
		//l := len(r.Body)
		//v.Set(reflect.MakeSlice(v.Type().Elem(), l, l))
		p := v.Addr().Interface().(*[]byte)
		*p = r.Tail()
	} else {
		r.Uint8slVal(v)
	}
}

func (r *Reader) Uint16slValTail(v reflect.Value) {
	if r.Err != nil {
		return
	}
	if v.CanAddr() {
		l := len(r.Body) / 2
		v.Set(reflect.MakeSlice(v.Type().Elem(), l, l))
	}
	r.Uint16slVal(v)
}

func (r *Reader) Uint32slValTail(v reflect.Value) {
	if r.Err != nil {
		return
	}
	if v.CanAddr() {
		l := len(r.Body) / 4
		v.Set(reflect.MakeSlice(v.Type().Elem(), l, l))
	}
	r.Uint32slVal(v)
}

func (r *Reader) Uint64slValTail(v reflect.Value) {
	if r.Err != nil {
		return
	}
	if v.CanAddr() {
		l := len(r.Body) / 8
		v.Set(reflect.MakeSlice(v.Type().Elem(), l, l))
	}
	r.Uint64slVal(v)
}

func (r *Reader) Int8slValTail(v reflect.Value) {
	if r.Err != nil {
		return
	}
	if v.CanAddr() {
		l := len(r.Body)
		v.Set(reflect.MakeSlice(v.Type().Elem(), l, l))
	}
	r.Int8slVal(v)
}

func (r *Reader) Int16slValTail(v reflect.Value) {
	if r.Err != nil {
		return
	}
	if v.CanAddr() {
		l := len(r.Body) / 2
		v.Set(reflect.MakeSlice(v.Type().Elem(), l, l))
	}
	r.Int16slVal(v)
}

func (r *Reader) Int32slValTail(v reflect.Value) {
	if r.Err != nil {
		return
	}
	if v.CanAddr() {
		l := len(r.Body) / 4
		v.Set(reflect.MakeSlice(v.Type().Elem(), l, l))
	}
	r.Int32slVal(v)
}

func (r *Reader) Int64slValTail(v reflect.Value) {
	if r.Err != nil {
		return
	}
	if v.CanAddr() {
		l := len(r.Body) / 8
		v.Set(reflect.MakeSlice(v.Type().Elem(), l, l))
	}
	r.Int64slVal(v)
}

func (r *Reader) Float32slValTail(v reflect.Value) {
	if r.Err != nil {
		return
	}
	if v.CanAddr() {
		l := len(r.Body) / 4
		v.Set(reflect.MakeSlice(v.Type().Elem(), l, l))
	}
	r.Float32slVal(v)
}

func (r *Reader) Float64slValTail(v reflect.Value) {
	if r.Err != nil {
		return
	}
	if v.CanAddr() {
		l := len(r.Body) / 8
		v.Set(reflect.MakeSlice(v.Type().Elem(), l, l))
	}
	r.Float64slVal(v)
}
