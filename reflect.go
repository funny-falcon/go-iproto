package iproto

import (
	"reflect"
	"math"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
)

var _ = log.Print

const (
	wDefaultBuf = 512
)

var le = binary.LittleEndian

type IWriter interface {
	IWrite(self interface{}, w *Writer) error
}
type IReader interface {
	IRead(self interface{}, r Reader) (rest Reader, err error)
}

type Writer struct {
	buf []byte
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
		n = (n-1) >> 2
	}
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return (n+1) << 2
}

func (w *Writer) ensure(n int) (l int) {
	if cap(w.buf) - len(w.buf) < n {
		newCap := len(w.buf) + n
		if newCap < wDefaultBuf {
			newCap = wDefaultBuf
		}
		tmp := make([]byte, len(w.buf), ceilLog(newCap))
		copy(tmp, w.buf)
		w.buf = tmp
	}
	l = len(w.buf)
	w.buf = w.buf[:len(w.buf)+n]
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
		w.buf[l+1] = uint8(i&0x7f)
		return
	case i < 1<<21:
		l := w.ensure(3)
		w.buf[l] = 0x80 | uint8(i>>14)
		w.buf[l+1] = 0x80 | uint8((i>>7)&0x7f)
		w.buf[l+2] = uint8(i&0x7f)
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
	j := l+n-1
	w.buf[j] = uint8(i&0x7f)
	for k := n-1; k!=0; k-- {
		i >>= 7
		j--
		w.buf[j] = 0x80 | uint8(i&0x7f)
	}
}

func (w *Writer) Intvar(i int) {
	w.Uint64var(uint64(i))
}

func (w *Writer) Bytesl(i []byte) {
	l := w.ensure(len(i))
	copy(w.buf[l:], i)
	return
}

func (w *Writer) Uint8sl(i []uint8) {
	l := w.ensure(len(i))
	copy(w.buf[l:], i)
	return
}

func (w *Writer) Int8sl(i []int8) {
	l := w.ensure(len(i))
	for j:=0; j<len(i); j++ {
		w.buf[l+j] = uint8(i[j])
	}
	return
}

func (w *Writer) Uint16sl(i []uint16) {
	l := w.ensure(len(i)*2)
	for j:=0; j<len(i); j++ {
		le.PutUint16(w.buf[l+j*2:], i[j])
	}
	return
}

func (w *Writer) Int16sl(i []int16) {
	l := w.ensure(len(i)*2)
	for j:=0; j<len(i); j++ {
		le.PutUint16(w.buf[l+j*2:], uint16(i[j]))
	}
	return
}

func (w *Writer) Uint32sl(i []uint32) {
	l := w.ensure(len(i)*4)
	for j:=0; j<len(i); j++ {
		le.PutUint32(w.buf[l+j*4:], i[j])
	}
	return
}

func (w *Writer) Int32sl(i []int32) {
	l := w.ensure(len(i)*4)
	for j:=0; j<len(i); j++ {
		le.PutUint32(w.buf[l+j*4:], uint32(i[j]))
	}
	return
}

func (w *Writer) Uint64sl(i []uint64) {
	l := w.ensure(len(i)*8)
	for j:=0; j<len(i); j++ {
		le.PutUint64(w.buf[l+j*8:], i[j])
	}
	return
}

func (w *Writer) Int64sl(i []int64) {
	l := w.ensure(len(i)*8)
	for j:=0; j<len(i); j++ {
		le.PutUint64(w.buf[l+j*8:], uint64(i[j]))
	}
	return
}

func (w *Writer) Float32sl(i []float32) {
	l := w.ensure(len(i)*4)
	for j:=0; j<len(i); j++ {
		le.PutUint32(w.buf[l+j*4:], math.Float32bits(i[j]))
	}
	return
}

func (w *Writer) Float64sl(i []float64) {
	l := w.ensure(len(i)*8)
	for j:=0; j<len(i); j++ {
		le.PutUint64(w.buf[l+j*4:], math.Float64bits(i[j]))
	}
	return
}

func (w *Writer) Write(i interface{}) (err error) {
	switch o := i.(type) {
	case uint8:
		w.Uint8(o)
	case int8:
		w.Int8(o)
	case uint16:
		w.Uint16(o)
	case int16:
		w.Int16(o)
	case uint32:
		w.Uint32(o)
	case int32:
		w.Int32(o)
	case uint64:
		w.Uint64(o)
	case int64:
		w.Int64(o)
	case *uint8:
		w.Uint8(*o)
	case *int8:
		w.Int8(*o)
	case *uint16:
		w.Uint16(*o)
	case *int16:
		w.Int16(*o)
	case *uint32:
		w.Uint32(*o)
	case *int32:
		w.Int32(*o)
	case *uint64:
		w.Uint64(*o)
	case *int64:
		w.Int64(*o)
	case []uint8:
		w.Uint8sl(o)
	case []int8:
		w.Int8sl(o)
	case []uint16:
		w.Uint16sl(o)
	case []int16:
		w.Int16sl(o)
	case []uint32:
		w.Uint32sl(o)
	case []int32:
		w.Int32sl(o)
	case []uint64:
		w.Uint64sl(o)
	case []int64:
		w.Int64sl(o)
	case IWriter:
		err = o.IWrite(o, w)
	case []IWriter:
		for _, v := range o {
			err = v.IWrite(v, w)
		}
	default:
		v := reflect.ValueOf(i)
		_, err = w.Reflect(v, iNotImplements)
	}
	if err != nil {
		w.Reset()
	}
	return
}

var _iwrite *IWriter = new(IWriter)
var iwriter = reflect.TypeOf(_iwrite).Elem()
var _iread *IReader = new(IReader)
var ireader = reflect.TypeOf(_iread).Elem()

type Implements int
const (
	iUnknown = Implements(iota)
	iImplements
	iNotImplements
)

func (w *Writer) Reflect(v reflect.Value, impl Implements) (imp Implements, err error) {
	if impl == iUnknown && v.Type().Implements(iwriter) || impl == iImplements {
		imp = iImplements
		o := v.Interface().(IWriter)
		err = o.IWrite(o, w)
		return
	}

	v = reflect.Indirect(v)
	t := v.Type()
	if t.Size() == 0 {
		return
	}

	switch v.Kind() {
	case reflect.Array:
		v = v.Slice(0, v.Len())
		fallthrough
	case reflect.Slice:
		et := t.Elem()
		switch et.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			err = w.Write(v.Interface())
			return
		}
		l := v.Len()
		impl = iUnknown
		for i:=0; i < l && err == nil; i++ {
			impl, err = w.Reflect(v.Index(i), impl)
		}

	case reflect.Struct:
		l := t.NumField()
		for i:=0; i < l && err == nil; i++ {
			_, err = w.Reflect(v.Field(i), iUnknown)
		}

	case reflect.Int8:
		w.Int8(int8(v.Int()))
	case reflect.Int16:
		w.Int16(int16(v.Int()))
	case reflect.Int32:
		w.Int32(int32(v.Int()))
	case reflect.Int64:
		w.Int64(v.Int())

	case reflect.Uint8:
		w.Uint8(uint8(v.Uint()))
	case reflect.Uint16:
		w.Uint16(uint16(v.Uint()))
	case reflect.Uint32:
		w.Uint32(uint32(v.Uint()))
	case reflect.Uint64:
		w.Uint64(v.Uint())

	case reflect.Float32:
		w.Uint32(math.Float32bits(float32(v.Float())))
	case reflect.Float64:
		w.Uint64(math.Float64bits(v.Float()))

	default:
		err = errors.New("iproto.Writer: wrong type "+v.Type().String())
	}
	return
}

type Reader []byte

func (r Reader) Uint8() (uint8, Reader, error) {
	if len(r) < 1 {
		return 0, nil, errors.New("iproto.Reader: not enough data for uint8")
	}
	return r[0], r[1:], nil
}

func (r Reader) Int8() (int8, Reader, error) {
	if len(r) < 1 {
		return 0, nil, errors.New("iproto.Reader: not enough data for int8")
	}
	return int8(r[0]), r[1:], nil
}

func (r Reader) Uint16() (uint16, Reader, error) {
	if len(r) < 2 {
		return 0, nil, errors.New("iproto.Reader: not enough data for uint16")
	}
	return le.Uint16(r), r[2:], nil
}

func (r Reader) Int16() (int16, Reader, error) {
	if len(r) < 2 {
		return 0, nil, errors.New("iproto.Reader: not enough data for int16")
	}
	return int16(le.Uint16(r)), r[2:], nil
}

func (r Reader) Uint32() (uint32, Reader, error) {
	if len(r) < 4 {
		return 0, nil, errors.New("iproto.Reader: not enough data for uint32")
	}
	return le.Uint32(r), r[4:], nil
}

func (r Reader) Int32() (int32, Reader, error) {
	if len(r) < 4 {
		return 0, nil, errors.New("iproto.Reader: not enough data for int32")
	}
	return int32(le.Uint32(r)), r[4:], nil
}

func (r Reader) Uint64() (uint64, Reader, error) {
	if len(r) < 8 {
		return 0, nil, errors.New("iproto.Reader: not enough data for uint64")
	}
	return le.Uint64(r), r[8:], nil
}

func (r Reader) Int64() (int64, Reader, error) {
	if len(r) < 8 {
		return 0, nil, errors.New("iproto.Reader: not enough data for int64")
	}
	return int64(le.Uint64(r)), r[8:], nil
}

func (r Reader) Float32() (float32, Reader, error) {
	if len(r) < 4 {
		return 0, nil, errors.New("iproto.Reader: not enough data for float32")
	}
	return math.Float32frombits(le.Uint32(r)), r[4:], nil
}

func (r Reader) Float64() (float64, Reader, error) {
	if len(r) < 8 {
		return 0, nil, errors.New("iproto.Reader: not enough data for float64")
	}
	return math.Float64frombits(le.Uint64(r)), r[8:], nil
}

func (r Reader) Uint64var() (uint64, Reader, error) {
	i, n := binary.Uvarint(r)
	if n > 0 {
		return i, r[n:], nil
	} else if n == 0 {
		return 0, nil, fmt.Errorf("iproto.Reader: not enough data for uint64var %x", r)
	} else {
		return 0, nil, fmt.Errorf("iproto.Reader: varint is too big %x", r)
	}
}

func (r Reader) Intvar() (int, Reader, error) {
	i, rest, err := r.Uint64var()
	return int(i), rest, err
}

func (r Reader) Uint8sl(b []uint8) (Reader, error) {
	if len(r) < len(b) {
		return nil, errors.New("iproto.Reader: not enough data for []uint8")
	}
	copy(b, r)
	return r[len(b):], nil
}

func (r Reader) Bytes(b []byte) (Reader, error) {
	if len(r) < len(b) {
		return nil, errors.New("iproto.Reader: not enough data for []byte")
	}
	copy(b, r)
	return r[len(b):], nil
}

func (r Reader) Int8sl(b []int8) (Reader, error) {
	if len(r) < len(b) {
		return nil, errors.New("iproto.Reader: not enough data for []uint8")
	}
	for i:=0; i<len(b); i++ {
		b[i] = int8(r[i])
	}
	return r[len(b):], nil
}

func (r Reader) Uint16sl(b []uint16) (Reader, error) {
	if len(r) < len(b)*2 {
		return nil, errors.New("iproto.Reader: not enough data for []uint16")
	}
	for i:=0; i<len(b); i++ {
		b[i] = le.Uint16(r[i*2:])
	}
	return r[len(b)*2:], nil
}

func (r Reader) Int16sl(b []int16) (Reader, error) {
	if len(r) < len(b)*2 {
		return nil, errors.New("iproto.Reader: not enough data for []int16")
	}
	for i:=0; i<len(b); i++ {
		b[i] = int16(le.Uint16(r[i*2:]))
	}
	return r[len(b)*2:], nil
}

func (r Reader) Uint32sl(b []uint32) (Reader, error) {
	if len(r) < len(b)*4 {
		return nil, errors.New("iproto.Reader: not enough data for []uint32")
	}
	for i:=0; i<len(b); i++ {
		b[i] = le.Uint32(r[i*4:])
	}
	return r[len(b)*4:], nil
}

func (r Reader) Int32sl(b []int32) (Reader, error) {
	if len(r) < len(b)*4 {
		return nil, errors.New("iproto.Reader: not enough data for []int32")
	}
	for i:=0; i<len(b); i++ {
		b[i] = int32(le.Uint32(r[i*4:]))
	}
	return r[len(b)*4:], nil
}

func (r Reader) Uint64sl(b []uint64) (Reader, error) {
	if len(r) < len(b)*8 {
		return nil, errors.New("iproto.Reader: not enough data for []uint64")
	}
	for i:=0; i<len(b); i++ {
		b[i] = le.Uint64(r[i*8:])
	}
	return r[len(b)*8:], nil
}

func (r Reader) Int64sl(b []int64) (Reader, error) {
	if len(r) < len(b)*8 {
		return nil, errors.New("iproto.Reader: not enough data for []int64")
	}
	for i:=0; i<len(b); i++ {
		b[i] = int64(le.Uint64(r[i*8:]))
	}
	return r[len(b)*8:], nil
}

func (r Reader) Read(i interface{}) (rest Reader, err error) {
	switch o := i.(type) {
	case *int8:
		*o, rest, err = r.Int8()
	case *uint8:
		*o, rest, err = r.Uint8()
	case *int16:
		*o, rest, err = r.Int16()
	case *uint16:
		*o, rest, err = r.Uint16()
	case *int32:
		*o, rest, err = r.Int32()
	case *uint32:
		*o, rest, err = r.Uint32()
	case *int64:
		*o, rest, err = r.Int64()
	case *uint64:
		*o, rest, err = r.Uint64()
	case []int8:
		rest, err = r.Int8sl(o)
	case []uint8:
		rest, err = r.Uint8sl(o)
	case []int16:
		rest, err = r.Int16sl(o)
	case []uint16:
		rest, err = r.Uint16sl(o)
	case []int32:
		rest, err = r.Int32sl(o)
	case []uint32:
		rest, err = r.Uint32sl(o)
	case []int64:
		rest, err = r.Int64sl(o)
	case []uint64:
		rest, err = r.Uint64sl(o)
	case IReader:
		rest, err = o.IRead(o, r)
	default:
		// Fallback to reflect-based decoding.
		var v reflect.Value
		switch d := reflect.ValueOf(i); d.Kind() {
		case reflect.Ptr:
			v = d.Elem()
		case reflect.Slice:
			v = d
		default:
			return nil, errors.New("iproto.Reader: invalid type " + d.Type().String())
		}
		_, rest, err = r.Reflect(v, iNotImplements)
	}
	return
}

func (r Reader) Reflect(v reflect.Value, impl Implements) (imp Implements, rest Reader, err error) {
	if impl == iUnknown && v.Type().Implements(ireader) || impl == iImplements {
		imp = iImplements
		v := v.Interface().(IReader)
		rest, err = v.IRead(v, r)
		return
	}

	switch v.Kind() {
	case reflect.Array:
		v = v.Slice(0, v.Len())
		fallthrough

	case reflect.Slice:
		switch et := v.Type().Elem(); et.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			rest, err = r.Read(v.Interface())
			return
		}

		l := v.Len()
		rest = r
		impl = iUnknown
		for i := 0; i < l && err != nil; i++ {
			impl, rest, err = rest.Reflect(v.Index(i), impl)
		}

	case reflect.Struct:
		l := v.NumField()
		rest = r
		for i := 0; i < l; i++ {
			if v := v.Field(i); v.CanSet() {
				_, rest, err = rest.Reflect(v, iUnknown)
			}
		}

	case reflect.Int8:
		var i int8
		if i, rest, err = r.Int8(); err == nil {
			v.SetInt(int64(i))
		}
	case reflect.Int16:
		var i int16
		if i, rest, err = r.Int16(); err == nil {
			v.SetInt(int64(i))
		}
	case reflect.Int32:
		var i int32
		if i, rest, err = r.Int32(); err == nil {
			v.SetInt(int64(i))
		}
	case reflect.Int64:
		var i int64
		if i, rest, err = r.Int64(); err == nil {
			v.SetInt(int64(i))
		}

	case reflect.Uint8:
		var i uint8
		if i, rest, err = r.Uint8(); err == nil {
			v.SetUint(uint64(i))
		}
	case reflect.Uint16:
		var i uint16
		if i, rest, err = r.Uint16(); err == nil {
			v.SetUint(uint64(i))
		}
	case reflect.Uint32:
		var i uint32
		if i, rest, err = r.Uint32(); err == nil {
			v.SetUint(uint64(i))
		}
	case reflect.Uint64:
		var i uint64
		if i, rest, err = r.Uint64(); err == nil {
			v.SetUint(uint64(i))
		}

	case reflect.Float32:
		var i float32
		if i, rest, err = r.Float32(); err == nil {
			v.SetFloat(float64(i))
		}
	case reflect.Float64:
		var i float64
		if i, rest, err = r.Float64(); err == nil {
			v.SetFloat(i)
		}
	default:
		err = errors.New("iproto.Writer: wrong type "+v.Type().String())
	}
	return
}
