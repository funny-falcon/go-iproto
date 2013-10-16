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
	if l = int(r.Uint32()); l == 1 {
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

func ReadTuple(r *marshal.Reader, i interface{}) error {
	switch o := i.(type) {
	case nil:
		return nil
	case *uint8:
		if oneFieldTuple(r, 1) {
			*o = r.Uint8()
		}
	case *int8:
		if oneFieldTuple(r, 1) {
			*o = r.Int8()
		}
	case *uint16:
		if oneFieldTuple(r, 2) {
			*o = r.Uint16()
		}
	case *int16:
		if oneFieldTuple(r, 2) {
			*o = r.Int16()
		}
	case *uint32:
		if oneFieldTuple(r, 4) {
			*o = r.Uint32()
		}
	case *int32:
		if oneFieldTuple(r, 4) {
			*o = r.Int32()
		}
	case *uint64:
		if oneFieldTuple(r, 8) {
			*o = r.Uint64()
		}
	case *int64:
		if oneFieldTuple(r, 8) {
			*o = r.Int64()
		}
	case *float32:
		if oneFieldTuple(r, 4) {
			*o = r.Float32()
		}
	case *float64:
		if oneFieldTuple(r, 8) {
			*o = r.Float64()
		}
	case *string:
		if l := r.Uint32(); l == 1 {
			sz := r.Intvar()
			*o = r.String(sz)
		} else {
			r.Err = fmt.Errorf("Wrong field count: expect 1, got %d", l)
		}
	case []byte:
		if oneFieldTuple(r, len(o)) {
			r.Uint8sl(o)
		}
	case *[]byte:
		if l := r.Uint32(); l == 1 {
			sz := r.Intvar()
			*o = r.Slice(sz)
		} else {
			r.Err = fmt.Errorf("Wrong field count: expect 1, got %d", l)
		}
	case [][]byte:
		if l := int(r.Uint32()); l == len(o) {
			for i := 0; i < l; i++ {
				sz := r.Intvar()
				o[i] = r.Slice(sz)
			}
		} else {
			r.Err = fmt.Errorf("Wrong field count: expect %d, got %d", len(o), l)
		}
	case *[][]byte:
		l := int(r.Uint32())
		*o = make([][]byte, l)
		for i := 0; i < l; i++ {
			sz := r.Intvar()
			(*o)[i] = r.Slice(sz)
		}
	case []interface{}:
		if l := int(r.Uint32()); l == len(o) {
			for i := 0; i < l; i++ {
				r.ReadWithSize(o[i], (*marshal.Reader).Intvar)
			}
		} else {
			r.Err = fmt.Errorf("Wrong field count: expect %d, got %d", len(o), l)
		}
	default:
		val := reflect.ValueOf(i)
		if val.Kind() == reflect.Ptr {
			val := val.Elem()
			rt := val.Type()
			rd := reader(rt)
			rd.Auto(r, val)
		} else {
			rt := val.Type()
			rd := reader(rt)
			rd.Fixed(r, val)
		}
	}
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
	if l := int(r.Uint32()); l == v.Len() {
		for i := 0; i < l; i++ {
			t.Reader.Elem.WithSize(r, v.Index(i), (*marshal.Reader).Intvar)
		}
	} else {
		r.Err = fmt.Errorf("Wrong field count: expect %d, got %d", v.Len(), l)
	}
}

func (t *TReader) sliceAuto(r *marshal.Reader, v reflect.Value) {
	l := int(r.Uint32())
	if v.CanSet() {
		v.Set(reflect.MakeSlice(v.Type(), l, l))
	} else if l != v.Len() {
		r.Err = fmt.Errorf("Wrong field count: expect %d, got %d", v.Len(), l)
		return
	}
	for i := 0; i < l; i++ {
		t.Reader.Elem.WithSize(r, v.Index(i), (*marshal.Reader).Intvar)
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
		t.Fixed = t.sliceFixed
		t.Auto = t.sliceFixed
	case reflect.Slice:
		t.Fixed = t.sliceFixed
		t.Auto = t.sliceAuto
	case reflect.Struct:
		t.FillStruct()
	default:
		log.Panicf("Don't know how to read type %+v as a tuple", rt)
	}
}

func (sw *TReader) structFixed(r *marshal.Reader, v reflect.Value) {
	flds := sw.Reader.Flds
	l := int(r.Uint32())
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
	l := int(r.Uint32())
	switch sw.Tail {
	case NoTail:
		if l != len(flds) {
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
			if tail%llast != 0 {
				r.Err = fmt.Errorf("Wrong field count: expect to be %d+n*%d, got %d", len(flds)-1, llast, l)
				return
			}
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
