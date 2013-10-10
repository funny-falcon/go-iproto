package marshal

import (
	"errors"
	"math"
	"reflect"
	"unsafe"
)

func (r *Reader) Uint16() (res uint16) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < 2 {
		r.Err = errors.New("iproto.Reader: not enough data for uint16")
		return
	}
	res = le.Uint16(r.Body)
	r.Body = r.Body[2:]
	return
}

func (r *Reader) Int16() (res int16) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < 2 {
		r.Err = errors.New("iproto.Reader: not enough data for int16")
		return
	}
	res = int16(le.Uint16(r.Body))
	r.Body = r.Body[2:]
	return
}

func (r *Reader) Uint32() (res uint32) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < 4 {
		r.Err = errors.New("iproto.Reader: not enough data for uint32")
		return
	}
	res = le.Uint32(r.Body)
	r.Body = r.Body[4:]
	return
}

func (r *Reader) Int32() (res int32) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < 4 {
		r.Err = errors.New("iproto.Reader: not enough data for int32")
		return
	}
	res = int32(le.Uint32(r.Body))
	r.Body = r.Body[4:]
	return
}

func (r *Reader) Uint64() (res uint64) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < 8 {
		r.Err = errors.New("iproto.Reader: not enough data for uint64")
		return
	}
	res = le.Uint64(r.Body)
	r.Body = r.Body[8:]
	return
}

func (r *Reader) Int64() (res int64) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < 8 {
		r.Err = errors.New("iproto.Reader: not enough data for int64")
		return
	}
	res = int64(le.Uint64(r.Body))
	r.Body = r.Body[8:]
	return
}

func (r *Reader) Float32() (res float32) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < 4 {
		r.Err = errors.New("iproto.Reader: not enough data for float32")
		return
	}
	res = math.Float32frombits(le.Uint32(r.Body))
	r.Body = r.Body[4:]
	return
}

func (r *Reader) Float64() (res float64) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < 8 {
		r.Err = errors.New("iproto.Reader: not enough data for float64")
		return
	}
	res = math.Float64frombits(le.Uint64(r.Body))
	r.Body = r.Body[8:]
	return
}

func (r *Reader) Uint16sl(b []uint16) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < len(b)*2 {
		r.Err = errors.New("iproto.Reader: not enough data for []uint16")
		return
	}
	for i := 0; i < len(b); i++ {
		b[i] = le.Uint16(r.Body[i*2:])
	}
	r.Body = r.Body[len(b)*2:]
	return
}

func (r *Reader) Int16sl(b []int16) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < len(b)*2 {
		r.Err = errors.New("iproto.Reader: not enough data for []int16")
		return
	}
	for i := 0; i < len(b); i++ {
		b[i] = int16(le.Uint16(r.Body[i*2:]))
	}
	r.Body = r.Body[len(b)*2:]
	return
}

func (r *Reader) Uint32sl(b []uint32) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < len(b)*4 {
		r.Err = errors.New("iproto.Reader: not enough data for []uint32")
		return
	}
	for i := 0; i < len(b); i++ {
		b[i] = le.Uint32(r.Body[i*4:])
	}
	r.Body = r.Body[len(b)*4:]
	return
}

func (r *Reader) Int32sl(b []int32) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < len(b)*4 {
		r.Err = errors.New("iproto.Reader: not enough data for []int32")
		return
	}
	for i := 0; i < len(b); i++ {
		b[i] = int32(le.Uint32(r.Body[i*4:]))
	}
	r.Body = r.Body[len(b)*4:]
	return
}

func (r *Reader) Uint64sl(b []uint64) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < len(b)*8 {
		r.Err = errors.New("iproto.Reader: not enough data for []uint64")
		return
	}
	for i := 0; i < len(b); i++ {
		b[i] = le.Uint64(r.Body[i*8:])
	}
	r.Body = r.Body[len(b)*8:]
	return
}

func (r *Reader) Int64sl(b []int64) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < len(b)*8 {
		r.Err = errors.New("iproto.Reader: not enough data for []int64")
		return
	}
	for i := 0; i < len(b); i++ {
		b[i] = int64(le.Uint64(r.Body[i*8:]))
	}
	r.Body = r.Body[len(b)*8:]
	return
}

func (r *Reader) Float32sl(b []float32) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < len(b)*4 {
		r.Err = errors.New("iproto.Reader: not enough data for []float32")
		return
	}
	for i := 0; i < len(b); i++ {
		b[i] = math.Float32frombits(le.Uint32(r.Body[i*4:]))
	}
	r.Body = r.Body[len(b)*4:]
	return
}

func (r *Reader) Float64sl(b []float64) {
	if r.Err != nil {
		return
	}
	if len(r.Body) < len(b)*8 {
		r.Err = errors.New("iproto.Reader: not enough data for []float64")
		return
	}
	for i := 0; i < len(b); i++ {
		b[i] = math.Float64frombits(le.Uint64(r.Body[i*8:]))
	}
	r.Body = r.Body[len(b)*8:]
	return
}

func (r *Reader) Uint8slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		p := (*[gg]uint8)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
		r.Uint8sl(p[:l])
	}
}

func (r *Reader) Int8slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		p := (*[gg]uint8)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
		r.Uint8sl(p[:l])
	}
}

func (r *Reader) Uint16slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		p := (*[gg]uint16)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
		r.Uint16sl(p[:l])
	}
}

func (r *Reader) Uint32slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		p := (*[gg]uint32)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
		r.Uint32sl(p[:l])
	}
}

func (r *Reader) Uint64slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		p := (*[gg]uint64)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
		r.Uint64sl(p[:l])
	}
}

func (r *Reader) Int16slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		p := (*[gg]int16)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
		r.Int16sl(p[:l])
	}
}

func (r *Reader) Int32slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		p := (*[gg]int32)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
		r.Int32sl(p[:l])
	}
}

func (r *Reader) Int64slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		p := (*[gg]int64)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
		r.Int64sl(p[:l])
	}
}

func (r *Reader) Float32slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		p := (*[gg]float32)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
		r.Float32sl(p[:l])
	}
}

func (r *Reader) Float64slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		p := (*[gg]float64)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
		r.Float64sl(p[:l])
	}
}
