package marshal

import (
	"math"
	"reflect"
	"unsafe"
)

func (w *Writer) Uint16(i uint16) {
	l := w.ensure(2)
	le.PutUint16(w.buf[l:], i)
	return
}

func (w *Writer) Int16(i int16) {
	l := w.ensure(2)
	le.PutUint16(w.buf[l:], uint16(i))
	return
}

func (w *Writer) Uint32(i uint32) {
	l := w.ensure(4)
	le.PutUint32(w.buf[l:], i)
	return
}

func (w *Writer) Int32(i int32) {
	l := w.ensure(4)
	le.PutUint32(w.buf[l:], uint32(i))
	return
}

func (w *Writer) Uint64(i uint64) {
	l := w.ensure(8)
	le.PutUint64(w.buf[l:], i)
	return
}

func (w *Writer) Int64(i int64) {
	l := w.ensure(8)
	le.PutUint64(w.buf[l:], uint64(i))
	return
}

func (w *Writer) Float32(i float32) {
	l := w.ensure(4)
	le.PutUint32(w.buf[l:], math.Float32bits(i))
	return
}

func (w *Writer) Float64(i float64) {
	l := w.ensure(8)
	le.PutUint64(w.buf[l:], math.Float64bits(i))
	return
}

func (w *Writer) Uint16sl(i []uint16) {
	l := w.ensure(len(i) * 2)
	for j := 0; j < len(i); j++ {
		le.PutUint16(w.buf[l+j*2:], i[j])
	}
	return
}

func (w *Writer) Int16sl(i []int16) {
	l := w.ensure(len(i) * 2)
	for j := 0; j < len(i); j++ {
		le.PutUint16(w.buf[l+j*2:], uint16(i[j]))
	}
	return
}

func (w *Writer) Uint32sl(i []uint32) {
	l := w.ensure(len(i) * 4)
	for j := 0; j < len(i); j++ {
		le.PutUint32(w.buf[l+j*4:], i[j])
	}
	return
}

func (w *Writer) Int32sl(i []int32) {
	l := w.ensure(len(i) * 4)
	for j := 0; j < len(i); j++ {
		le.PutUint32(w.buf[l+j*4:], uint32(i[j]))
	}
	return
}

func (w *Writer) Uint64sl(i []uint64) {
	l := w.ensure(len(i) * 8)
	for j := 0; j < len(i); j++ {
		le.PutUint64(w.buf[l+j*8:], i[j])
	}
	return
}

func (w *Writer) Int64sl(i []int64) {
	l := w.ensure(len(i) * 8)
	for j := 0; j < len(i); j++ {
		le.PutUint64(w.buf[l+j*8:], uint64(i[j]))
	}
	return
}

func (w *Writer) Float32sl(i []float32) {
	l := w.ensure(len(i) * 4)
	for j := 0; j < len(i); j++ {
		le.PutUint32(w.buf[l+j*4:], math.Float32bits(i[j]))
	}
	return
}

func (w *Writer) Float64sl(i []float64) {
	l := w.ensure(len(i) * 8)
	for j := 0; j < len(i); j++ {
		le.PutUint64(w.buf[l+j*4:], math.Float64bits(i[j]))
	}
	return
}

func (w *Writer) Uint16slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		if v.Index(0).CanAddr() {
			p := (*[gg]uint16)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
			w.Uint16sl(p[:l])
		} else {
			for i := 0; i < l; i++ {
				w.Uint16(uint16(v.Index(i).Uint()))
			}
		}
	}
}

func (w *Writer) Uint32slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		if v.Index(0).CanAddr() {
			p := (*[gg]uint32)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
			w.Uint32sl(p[:l])
		} else {
			for i := 0; i < l; i++ {
				w.Uint32(uint32(v.Index(i).Uint()))
			}
		}
	}
}

func (w *Writer) Uint64slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		if v.Index(0).CanAddr() {
			p := (*[gg]uint64)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
			w.Uint64sl(p[:l])
		} else {
			for i := 0; i < l; i++ {
				w.Uint64(uint64(v.Index(i).Uint()))
			}
		}
	}
}

func (w *Writer) Int16slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		if v.Index(0).CanAddr() {
			p := (*[gg]int16)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
			w.Int16sl(p[:l])
		} else {
			for i := 0; i < l; i++ {
				w.Int16(int16(v.Index(i).Int()))
			}
		}
	}
}

func (w *Writer) Int32slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		if v.Index(0).CanAddr() {
			p := (*[gg]int32)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
			w.Int32sl(p[:l])
		} else {
			for i := 0; i < l; i++ {
				w.Int32(int32(v.Index(i).Int()))
			}
		}
	}
}

func (w *Writer) Int64slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		if v.Index(0).CanAddr() {
			p := (*[gg]int64)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
			w.Int64sl(p[:l])
		} else {
			for i := 0; i < l; i++ {
				w.Int64(int64(v.Index(i).Int()))
			}
		}
	}
}

func (w *Writer) Float32slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		if v.Index(0).CanAddr() {
			p := (*[gg]float32)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
			w.Float32sl(p[:l])
		} else {
			for i := 0; i < l; i++ {
				w.Float32(float32(v.Index(i).Float()))
			}
		}
	}
}

func (w *Writer) Float64slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		if v.Index(0).CanAddr() {
			p := (*[gg]float64)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
			w.Float64sl(p[:l])
		} else {
			for i := 0; i < l; i++ {
				w.Float64(float64(v.Index(i).Float()))
			}
		}
	}
}
