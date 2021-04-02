package marshal

import (
	"log"
	"reflect"
	"unsafe"
)

var _ = log.Print

const (
	wDefaultBuf = 512
)

type Writer struct {
	buf     []byte
	DefSize int
}

func (w *Writer) Written() (res []byte) {
	res = w.buf
	w.buf = w.buf[len(w.buf):]
	return
}

func (w *Writer) Reset() {
	w.buf = w.buf[:0]
	return
}

func ceilLog(n int) int {
	if n > 0 {
		n = (n - 1) >> 2
	}
	n |= n >> 3
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return (n + 1) << 2
}

func (w *Writer) ensure(n int) (l int) {
	l = len(w.buf)
	if cap(w.buf)-l < n {
		newCap := l + n
		if w.DefSize == 0 {
			w.DefSize = wDefaultBuf
		}
		if newCap <= w.DefSize {
			newCap = w.DefSize
		} else {
			newCap = ceilLog(newCap)
		}
		tmp := make([]byte, l, newCap)
		copy(tmp, w.buf)
		w.buf = tmp
	}
	w.buf = w.buf[:l+n]
	return
}

func (w *Writer) Need(n int) []byte {
	l := w.ensure(n)
	return w.buf[l:]
}

func (w *Writer) Uint8(i uint8) {
	l := w.ensure(1)
	w.buf[l] = i
	return
}

func (w *Writer) Int8(i int8) {
	l := w.ensure(1)
	w.buf[l] = uint8(i)
	return
}

func varu64size(i uint64) (j int) {
	for j = 0; i > 1<<7; j++ {
		i >>= 7
	}
	return j + 1
}

func (w *Writer) Uint64var(i uint64) {
	var n int
	switch {
	case i < 1<<7:
		l := w.ensure(1)
		w.buf[l] = uint8(i)
		return
	case i < 1<<14:
		l := w.ensure(2)
		w.buf[l] = 0x80 | uint8(i>>7)
		w.buf[l+1] = uint8(i & 0x7f)
		return
	case i < 1<<21:
		l := w.ensure(3)
		w.buf[l] = 0x80 | uint8(i>>14)
		w.buf[l+1] = 0x80 | uint8((i>>7)&0x7f)
		w.buf[l+2] = uint8(i & 0x7f)
		return
	case i < 1<<28:
		n = 4
	case i < 1<<35:
		n = 5
	case i < 1<<42:
		n = 6
	case i < 1<<49:
		n = 7
	case i < 1<<56:
		n = 8
	case i < 1<<63:
		n = 9
	default:
		n = 10
	}
	l := w.ensure(n)
	j := l + n - 1
	w.buf[j] = uint8(i & 0x7f)
	for k := n - 1; k != 0; k-- {
		i >>= 7
		j--
		w.buf[j] = 0x80 | uint8(i&0x7f)
	}
}

func (w *Writer) Intvar(i int) {
	w.Uint64var(uint64(i))
}

func (w *Writer) IntUint64(i int) {
	w.Uint32(uint32(i))
}

func (w *Writer) IntUint32(i int) {
	w.Uint32(uint32(i))
}

func (w *Writer) IntUint16(i int) {
	w.Uint16(uint16(i))
}

func (w *Writer) IntUint8(i int) {
	w.Uint8(uint8(i))
}

func (w *Writer) Bytes(i []byte) {
	l := w.ensure(len(i))
	copy(w.buf[l:], i)
	return
}

func (w *Writer) Uint8sl(i []uint8) {
	l := w.ensure(len(i))
	copy(w.buf[l:], i)
	return
}

func (w *Writer) String(i string) {
	l := w.ensure(len(i))
	copy(w.buf[l:], i)
	return
}

func (w *Writer) Int8sl(i []int8) {
	l := w.ensure(len(i))
	for j := 0; j < len(i); j++ {
		w.buf[l+j] = uint8(i[j])
	}
	return
}

func (w *Writer) Int8Val(v reflect.Value) {
	w.Int8(int8(v.Int()))
}

func (w *Writer) Int16Val(v reflect.Value) {
	w.Int16(int16(v.Int()))
}

func (w *Writer) Int32Val(v reflect.Value) {
	w.Int32(int32(v.Int()))
}

func (w *Writer) Int64Val(v reflect.Value) {
	w.Int64(int64(v.Int()))
}

func (w *Writer) Uint8Val(v reflect.Value) {
	w.Uint8(uint8(v.Uint()))
}

func (w *Writer) Uint16Val(v reflect.Value) {
	w.Uint16(uint16(v.Uint()))
}

func (w *Writer) Uint32Val(v reflect.Value) {
	w.Uint32(uint32(v.Uint()))
}

func (w *Writer) Uint64Val(v reflect.Value) {
	w.Uint64(uint64(v.Uint()))
}

func (w *Writer) Float32Val(v reflect.Value) {
	w.Float32(float32(v.Float()))
}

func (w *Writer) Float64Val(v reflect.Value) {
	w.Float64(float64(v.Float()))
}

func (w *Writer) VarVal(v reflect.Value) {
	w.Uint64var(uint64(v.Uint()))
}

func (w *Writer) StringVal(v reflect.Value) {
	w.String(v.String())
}

func (w *Writer) Uint8slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		if v.Index(0).CanAddr() {
                        _p := uintptr( (unsafe.Pointer(v.Index(0).Addr().Pointer())) )
                        sh := &reflect.SliceHeader{Data: _p, Len:  l, Cap:  l, }
                        p := *(*[]uint8)(unsafe.Pointer(sh))
			w.Uint8sl(p[:l])
		} else {
			for i := 0; i < l; i++ {
				w.Uint8(uint8(v.Index(i).Uint()))
			}
		}
	}
}

func (w *Writer) Int8slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		if v.Index(0).CanAddr() {
                        _p := uintptr( (unsafe.Pointer(v.Index(0).Addr().Pointer())) )
                        sh := &reflect.SliceHeader{Data: _p, Len:  l, Cap:  l, }
                        p := *(*[]uint8)(unsafe.Pointer(sh))
			w.Uint8sl(p[:l])
		} else {
			for i := 0; i < l; i++ {
				w.Int8(int8(v.Index(i).Int()))
			}
		}
	}
}

func (w *Writer) VarslVal(v reflect.Value) {
	l := v.Len()
	for i := 0; i < l; i++ {
		w.Uint64var(v.Index(i).Uint())
	}
}
