package sbox

import (
	"fmt"
	"github.com/funny-falcon/go-iproto/marshal"
	"log"
	"reflect"
	"strings"
	"sync"
)

var _ = log.Print

func oneFieldTuple(r *marshal.Reader, sz int) bool {
	var l, s int
	if l = r.IntUint32(); l >= 1 {
		if s = r.Intvar(); s == sz {
			return true
		} else {
			r.Err = fmt.Errorf("Wrong field size: expect 1, got %d", l)
		}
	} else {
		r.Err = fmt.Errorf("Wrong field count: expect 1, got %d", l)
	}
	return false
}

func ReadSizedTuple(r *marshal.Reader, i interface{}) error {
	sz := r.IntUint32()
	if r.Err == nil {
		rd := marshal.Reader{Body: r.Slice(sz + 4)}
		r.Err = ReadRawTuple(&rd, i)
	}
	return r.Err
}

func ReadRawTuple(r *marshal.Reader, i interface{}) error {
	if i == nil {
		return nil
	}
	val := reflect.ValueOf(i)
	rt := val.Type()
	rd := reader(rt)
	rd.Fixed(r, val)
	return r.Err
}

var rs = make(map[uintptr]*TReader)
var rss = rs
var rsL sync.Mutex

func reader(rt reflect.Type) (rd *TReader) {
	rtid := reflect.ValueOf(rt).Pointer()
	if rd = rs[rtid]; rd == nil {
		rsL.Lock()
		defer rsL.Unlock()
		if rd = rs[rtid]; rd == nil {
			rss = make(map[uintptr]*TReader, len(ws)+1)
			for t, r := range rs {
				rss[t] = r
			}
			rd = _reader(rt)
			rs = rss
		}
	}
	return
}

func _reader(rt reflect.Type) (rd *TReader) {
	rtid := reflect.ValueOf(rt).Pointer()
	if rd = rss[rtid]; rd == nil {
		rd = &TReader{Reader: marshal.ReaderFor(rt)}
		rss[rtid] = rd
		rd.Fill()
	}
	return
}

type TReader struct {
	Reader *marshal.TReader
	Fixed  func(*marshal.Reader, reflect.Value)
	Auto   func(*marshal.Reader, reflect.Value)
	Tail   TailType
}

func (t *TReader) sliceFixed(r *marshal.Reader, v reflect.Value) {
	if l := r.IntUint32(); l >= v.Len() {
		for i := 0; i < l; i++ {
			t.Reader.Elem.WithSize(r, v.Index(i), (*marshal.Reader).Intvar)
		}
	} else {
		r.Err = fmt.Errorf("Wrong field count: expect %d, got %d", v.Len(), l)
	}
}

func (t *TReader) sliceAuto(r *marshal.Reader, v reflect.Value) {
	l := r.IntUint32()
	if v.CanSet() {
		v.Set(reflect.MakeSlice(v.Type(), l, l))
	} else if l < v.Len() {
		r.Err = fmt.Errorf("Wrong field count: expect %d, got %d", v.Len(), l)
		return
	}
	for i := 0; i < l; i++ {
		t.Reader.Elem.WithSize(r, v.Index(i), (*marshal.Reader).Intvar)
	}
}

func (t *TReader) fixedFixed(r *marshal.Reader, v reflect.Value) {
	if oneFieldTuple(r, t.Reader.Sz) {
		t.Reader.Fixed(r, v)
	}
}

func (t *TReader) string(r *marshal.Reader, v reflect.Value) {
	if l := r.Uint32(); l >= 1 {
		sz := r.Intvar()
		v.SetString(r.String(sz))
	} else {
		r.Err = fmt.Errorf("Wrong field count: expect 1, got %d", l)
	}
}

func (t *TReader) bytesFixed(r *marshal.Reader, v reflect.Value) {
	if oneFieldTuple(r, v.Len()) {
		t.Reader.Fixed(r, v)
	}
}

func (t *TReader) bytesAuto(r *marshal.Reader, v reflect.Value) {
	if !v.CanSet() {
		t.bytesFixed(r, v)
		return
	}
	if l := r.IntUint32(); l >= 1 {
		t.Reader.WithSize(r, v, (*marshal.Reader).Intvar)
	} else {
		r.Err = fmt.Errorf("Wrong field count: expect 1, got %d", l)
	}
}

func (t *TReader) Fill() {
	rt := t.Reader.Type

	switch rt.Kind() {
	case reflect.Ptr:
		elrd := _reader(rt.Elem())
		t.Fixed = func(r *marshal.Reader, v reflect.Value) {
			if !v.IsNil() {
				elrd.Auto(r, v.Elem())
			}
		}
		t.Auto = func(r *marshal.Reader, v reflect.Value) {
			if v.IsNil() {
				if v.CanSet() {
					v.Set(reflect.New(rt.Elem()))
				}
			}
			if !v.IsNil() {
				elrd.Auto(r, v.Elem())
			}
		}
	case reflect.Array:
		if rt.Elem().Kind() != reflect.Uint8 {
			t.Fixed = t.sliceFixed
			t.Auto = t.sliceFixed
		} else {
			t.Fixed = t.fixedFixed
			t.Auto = t.fixedFixed
		}
	case reflect.Slice:
		if rt.Elem().Kind() != reflect.Uint8 {
			t.Fixed = t.sliceFixed
			t.Auto = t.sliceAuto
		} else {
			t.Fixed = t.bytesFixed
			t.Auto = t.bytesAuto
		}
	case reflect.Struct:
		t.FillStruct()
	case reflect.Int8, reflect.Uint8, reflect.Uint16, reflect.Int16,
		reflect.Uint32, reflect.Int32, reflect.Uint64, reflect.Int64,
		reflect.Float32, reflect.Float64:
		t.Fixed = t.fixedFixed
		t.Auto = t.fixedFixed
	case reflect.String:
		t.Fixed = t.string
		t.Auto = t.string
	default:
		log.Panicf("Don't know how to read type %+v as a tuple", rt)
	}
}

func (sw *TReader) structFixed(r *marshal.Reader, v reflect.Value) {
	flds := sw.Reader.Flds
	l := r.IntUint32()
	switch sw.Tail {
	case NoTail:
		if l != len(flds) {
			r.Err = fmt.Errorf("Wrong field count: expect %d, got %d", len(flds), l)
			return
		}
	case Tail:
		need := len(flds) - 1 + v.Field(flds[len(flds)-1].I).Len()
		if l != need {
			r.Err = fmt.Errorf("Wrong field count: expect %d, got %d", need, l)
			return
		}
	case TailSplit:
		last := flds[len(flds)-1]
		need := len(flds) - 1 + v.Field(last.I).Len()*len(last.TReader.Flds)
		if l != need {
			r.Err = fmt.Errorf("Wrong field count: expect %d, got %d", need, l)
			return
		}
	}
	sw.structRead(r, v)
}

func (sw *TReader) structAuto(r *marshal.Reader, v reflect.Value) {
	if !v.CanSet() {
		sw.structFixed(r, v)
		return
	}
	flds := sw.Reader.Flds
	l := r.IntUint32()
	switch sw.Tail {
	case NoTail:
		if l < len(flds) {
			r.Err = fmt.Errorf("Wrong field count: expect %d, got %d", len(flds), l)
			return
		}
	case Tail, TailSplit:
		if l < len(flds)-1 {
			r.Err = fmt.Errorf("Wrong field count: expect at least %d, got %d", len(flds)-1, l)
			return
		}
		tail := l - len(flds) + 1
		last := flds[len(flds)-1]
		if sw.Tail == TailSplit {
			llast := len(last.Flds)
			tail /= llast
		}
		last.TReader.SetCount(v.Field(last.I), tail)
	}
	sw.structRead(r, v)
}

func (sw *TReader) structRead(r *marshal.Reader, v reflect.Value) {
	flds := sw.Reader.Flds
	n := len(flds)
	if sw.Tail != NoTail {
		n -= 1
	}
	for i := 0; i < n; i++ {
		fs := &flds[i]
		fs.WithSize(r, v.Field(fs.I), (*marshal.Reader).Intvar)
	}
	switch sw.Tail {
	case NoTail:
	case Tail:
		fs := &flds[n]
		fv := v.Field(n)
		l := fv.Len()
		for i := 0; i < l; i++ {
			val := fv.Index(i)
			fs.WithSize(r, val, (*marshal.Reader).Intvar)
		}
	case TailSplit:
		fs := &flds[n]
		fv := v.Field(n)
		l := fv.Len()
		fss := fs.TReader.Flds
		fl := len(fss)
		for i := 0; i < l; i++ {
			str := fv.Index(i)
			for j := 0; j < fl; j++ {
				fss[i].WithSize(r, str.Field(i), (*marshal.Reader).Intvar)
			}
		}
	}
}

func (t *TReader) FillStruct() {
	t.Fixed = t.structFixed
	t.Auto = t.structAuto
	for i := range t.Reader.Flds {
		fld := &t.Reader.Flds[i]
		ipro := fld.Tag.Get("sbox")

		for _, m := range strings.Split(ipro, ",") {
			if t.Tail != NoTail {
				log.Panicf("Sbox Tail could be only last field in a struct %+v", t.Reader.Type)
			}
			if m == "tail" {
				if fld.Type.Kind() != reflect.Slice {
					log.Panicf("Could apply sbox:tail only for slices")
				}
				t.Tail = Tail
			} else if m == "tailsplit" {
				if fld.Type.Kind() != reflect.Slice {
					log.Panicf("Could apply sbox:tailsplit only for slices")
				}
				if fld.Type.Elem().Kind() != reflect.Struct {
					log.Panicf("Could apply sbox:tailsplit only for slices of struct")
				}
				t.Tail = TailSplit
			}
		}
	}
}
