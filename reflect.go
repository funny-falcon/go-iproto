package iproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"reflect"
	"sync/atomic"
)

var _ = log.Print
var _ = atomic.StoreUint32

const (
	wDefaultBuf = 512
)

var le = binary.LittleEndian

type IWriter interface {
	IWrite(self interface{}, w *Writer)
}
type IReader interface {
	IRead(self interface{}, r *Reader)
}

type Writer struct {
	buf     []byte
	defSize int
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
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return (n + 1) << 2
}

func (w *Writer) ensure(n int) (l int) {
	if cap(w.buf)-len(w.buf) < n {
		newCap := len(w.buf) + n
		if w.defSize == 0 {
			w.defSize = wDefaultBuf
		}
		if newCap < w.defSize {
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

func (w *Writer) IntUint32(i int) {
	w.Uint32(uint32(i))
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

func (w *Writer) Int8sl(i []int8) {
	l := w.ensure(len(i))
	for j := 0; j < len(i); j++ {
		w.buf[l+j] = uint8(i[j])
	}
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

func (w *Writer) Write(i interface{}) {
	switch o := i.(type) {
	case nil:
		return
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
	case float32:
		w.Float32(o)
	case float64:
		w.Float64(o)
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
	case *float32:
		w.Float32(*o)
	case *float64:
		w.Float64(*o)
	case []uint8:
		w.IntUint32(len(o))
		w.Uint8sl(o)
	case []int8:
		w.IntUint32(len(o))
		w.Int8sl(o)
	case []uint16:
		w.IntUint32(len(o))
		w.Uint16sl(o)
	case []int16:
		w.IntUint32(len(o))
		w.Int16sl(o)
	case []uint32:
		w.IntUint32(len(o))
		w.Uint32sl(o)
	case []int32:
		w.IntUint32(len(o))
		w.Int32sl(o)
	case []uint64:
		w.IntUint32(len(o))
		w.Uint64sl(o)
	case []int64:
		w.IntUint32(len(o))
		w.Int64sl(o)
	case []float32:
		w.IntUint32(len(o))
		w.Float32sl(o)
	case []float64:
		w.IntUint32(len(o))
		w.Float64sl(o)
	case IWriter:
		o.IWrite(o, w)
	case []IWriter:
		for _, v := range o {
			v.IWrite(v, w)
		}
	default:
		v := reflect.ValueOf(i)
		w.Reflect(v, iNotImplements)
	}
	return
}

func (w *Writer) NumSl(i interface{}) {
	switch o := i.(type) {
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
	case []float32:
		w.Float32sl(o)
	case []float64:
		w.Float64sl(o)
	default:
		v := reflect.ValueOf(o)
		log.Panicf("iproto.NumSl: wrong type " + v.Type().String())
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

func (w *Writer) Reflect(v reflect.Value, impl Implements) (imp Implements) {
	if impl == iUnknown && v.Type().Implements(iwriter) || impl == iImplements {
		imp = iImplements
		o := v.Interface().(IWriter)
		o.IWrite(o, w)
		return
	}

	v = reflect.Indirect(v)
	t := v.Type()
	if t.Size() == 0 {
		return
	}

	switch v.Kind() {
	case reflect.Slice:
		w.IntUint32(v.Len())
		fallthrough
	case reflect.Array:
		l := v.Len()
		if v.Kind() == reflect.Array {
			v = v.Slice(0, v.Len())
		}
		et := t.Elem()
		switch et.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			w.NumSl(v.Interface())
			return
		}
		impl = iUnknown
		for i := 0; i < l; i++ {
			impl = w.Reflect(v.Index(i), impl)
		}

	case reflect.Struct:
		l := t.NumField()
		for i := 0; i < l; i++ {
			w.Reflect(v.Field(i), iUnknown)
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
		log.Panicf("iproto.Wirter.Reflect: wrong type " + v.Type().String())
	}
	return
}

type Body []byte
func (b Body) IWrite(self interface{}, w *Writer) {
	w.Bytes([]byte(b))
}

func (b Body) Reader() (r Reader) {
	r.Body = b
	return
}

func (b Body) Read(i interface{}) (r Reader) {
	r.Body = b
	r.Read(i)
	return
}

var readerLock uint32 = 0
var reader Reader
func (b Body) ReadAll(i interface{}) error {
	if atomic.CompareAndSwapUint32(&readerLock, 0, 1) {
		reader.Body = b
		reader.Err = nil
		reader.Read(i)
		err := reader.Error()
		atomic.StoreUint32(&readerLock, 0)
		return err
	} else {
		r := Reader{Body: b}
		r.Read(i)
		return r.Error()
	}
}

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

func (r *Reader) Uint64var() (res uint64) {
	if r.Err != nil {
		return
	}
	i, n := binary.Uvarint(r.Body)
	if n > 0 {
		res = i
		r.Body = r.Body[n:]
		return
	} else if n == 0 {
		r.Err = fmt.Errorf("iproto.Reader: not enough data for uint64var %x", r)
		return
	} else {
		r.Err = fmt.Errorf("iproto.Reader: varint is too big %x", r)
		return
	}
}

func (r *Reader) Intvar() (res int) {
	i := r.Uint64var()
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

func (r *Reader) Rest() (res []byte) {
	if r.Err != nil {
		return
	}
	res = r.Body
	r.Body = nil
	return
}

func (r *Reader) Read(i interface{}) {
	switch o := i.(type) {
	case *int8:
		*o = r.Int8()
	case *uint8:
		*o = r.Uint8()
	case *int16:
		*o = r.Int16()
	case *uint16:
		*o = r.Uint16()
	case *int32:
		*o = r.Int32()
	case *uint32:
		*o = r.Uint32()
	case *int64:
		*o = r.Int64()
	case *uint64:
		*o = r.Uint64()
	case *float32:
		*o = r.Float32()
	case *float64:
		*o = r.Float64()
	case []int8:
		r.Int8sl(o)
	case []uint8:
		r.Uint8sl(o)
	case []int16:
		r.Int16sl(o)
	case []uint16:
		r.Uint16sl(o)
	case []int32:
		r.Int32sl(o)
	case []uint32:
		r.Uint32sl(o)
	case []int64:
		r.Int64sl(o)
	case []uint64:
		r.Uint64sl(o)
	case []float32:
		r.Float32sl(o)
	case []float64:
		r.Float64sl(o)
	case *[]int8:
		var count uint32
		if count = r.Uint32(); r.Err != nil {
			return
		}
		*o = make([]int8, count)
		r.Int8sl(*o)
	case *[]uint8:
		var count uint32
		if count = r.Uint32(); r.Err != nil {
			return
		}
		*o = make([]uint8, count)
		r.Uint8sl(*o)
	case *[]int16:
		var count uint32
		if count = r.Uint32(); r.Err != nil {
			return
		}
		*o = make([]int16, count)
		r.Int16sl(*o)
	case *[]uint16:
		var count uint32
		if count = r.Uint32(); r.Err != nil {
			return
		}
		*o = make([]uint16, count)
		r.Uint16sl(*o)
	case *[]int32:
		var count uint32
		if count = r.Uint32(); r.Err != nil {
			return
		}
		*o = make([]int32, count)
		r.Int32sl(*o)
	case *[]uint32:
		var count uint32
		if count = r.Uint32(); r.Err != nil {
			return
		}
		*o = make([]uint32, count)
		r.Uint32sl(*o)
	case *[]int64:
		var count uint32
		if count = r.Uint32(); r.Err != nil {
			return
		}
		*o = make([]int64, count)
		r.Int64sl(*o)
	case *[]uint64:
		var count uint32
		if count = r.Uint32(); r.Err != nil {
			return
		}
		*o = make([]uint64, count)
		r.Uint64sl(*o)
	case *[]float32:
		var count uint32
		if count = r.Uint32(); r.Err != nil {
			return
		}
		*o = make([]float32, count)
		r.Float32sl(*o)
	case *[]float64:
		var count uint32
		if count = r.Uint32(); r.Err != nil {
			return
		}
		*o = make([]float64, count)
		r.Float64sl(*o)
	case IReader:
		o.IRead(o, r)
	default:
		// Fallback to reflect-based decoding.
		var v reflect.Value
		switch d := reflect.ValueOf(i); d.Kind() {
		case reflect.Ptr:
			v = d.Elem()
		case reflect.Slice:
			v = d
		default:
			log.Panicf("iproto.Reader.Read: wrong type " + v.Type().String())
			return
		}
		r.Reflect(v, iNotImplements)
	}
	return
}

func (r *Reader) Reflect(v reflect.Value, impl Implements) (imp Implements) {
	if v.Type().Size() == 0 {
		return iNotImplements
	}

	if impl == iUnknown && v.Type().Implements(ireader) || impl == iImplements {
		imp = iImplements
		v := v.Interface().(IReader)
		v.IRead(v, r)
		return
	}

	switch v.Kind() {
	case reflect.Ptr:
		r.Reflect(v.Elem(), impl)
	case reflect.Slice:
		var count uint32
		if count = r.Uint32(); r.Err != nil {
			return
		}
		s := reflect.MakeSlice(v.Type(), int(count), int(count))
		v.Set(s)
		fallthrough
	case reflect.Array:
		if v.Kind() == reflect.Array {
			v = v.Slice(0, v.Len())
		}

		switch et := v.Type().Elem(); et.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			r.Read(v.Interface())
			return
		}

		l := v.Len()
		impl = iUnknown
		for i := 0; i < l && r.Err == nil; i++ {
			el := v.Index(i)
			impl = r.Reflect(el.Addr(), impl)
		}

	case reflect.Struct:
		l := v.NumField()
		for i := 0; i < l && r.Err == nil; i++ {
			if el := v.Field(i); /*el.CanSet()*/ true {
				r.Reflect(el, iUnknown)
			}
		}

	case reflect.Int8:
		v.SetInt(int64(r.Int8()))
	case reflect.Int16:
		v.SetInt(int64(r.Int16()))
	case reflect.Int32:
		v.SetInt(int64(r.Int32()))
	case reflect.Int64:
		v.SetInt(r.Int64())
	case reflect.Uint8:
		v.SetUint(uint64(r.Uint8()))
	case reflect.Uint16:
		v.SetUint(uint64(r.Uint16()))
	case reflect.Uint32:
		v.SetUint(uint64(r.Uint32()))
	case reflect.Uint64:
		v.SetUint(r.Uint64())
	case reflect.Float32:
		v.SetFloat(float64(r.Float32()))
	case reflect.Float64:
		v.SetFloat(r.Float64())
	default:
		log.Panicf("iproto.Reader.Reflect: wrong type " + v.Type().String())
	}
	return
}

func (r Reader) Done() bool {
	return len(r.Body) == 0 && r.Err == nil
}

func (r Reader) Error() error {
	if r.Err != nil {
		return r.Err
	} else if len(r.Body) > 0 {
		return fmt.Errorf("Unparsed body: [% x]", r.Body)
	}
	return nil
}

type Struct struct{}

func (s Struct) IWrite(self interface{}, w *Writer) {
	v := reflect.ValueOf(self)
	w.Reflect(v, iNotImplements)
}

func (s Struct) IRead(self interface{}, r *Reader) {
	var v reflect.Value
	switch d := reflect.ValueOf(self); d.Kind() {
	case reflect.Ptr:
		v = d.Elem()
	default:
		r.Err = errors.New("iproto.Struct.IRead: invalid type " + d.Type().String())
		return
	}
	r.Reflect(v, iNotImplements)
}

type iwriterWrap struct {
	i interface{}
}

func (wrap iwriterWrap) IWrite(o interface{}, w *Writer) {
	w.Write(wrap.i)
}

func Wrap2IWriter(i interface{}) IWriter {
	if wr, ok := i.(IWriter); ok {
		return wr
	} else {
		return iwriterWrap{i}
	}
}

type ireaderWrap struct {
	i interface{}
}

func (wrap ireaderWrap) IRead(o interface{}, r *Reader) {
	r.Read(wrap.i)
}

func Wrap2IReader(i interface{}) (IReader, error) {
	return ireaderWrap{i}, nil
}
