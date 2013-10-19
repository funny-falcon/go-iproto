package marshal

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
)

func (r *Reader) Read(i interface{}) error {
	if r.Err != nil {
		return r.Err
	}
	var count uint32
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
	case *string:
		if count = r.Uint32(); r.Err == nil {
			*o = r.String(int(count))
		}
	case *[]int8:
		if count = r.Uint32(); r.Err == nil {
			*o = make([]int8, count)
			r.Int8sl(*o)
		}
	case *[]uint8:
		if count = r.Uint32(); r.Err == nil {
			*o = r.Slice(int(count))
		}
	case *[]int16:
		if count = r.Uint32(); r.Err == nil {
			*o = make([]int16, count)
			r.Int16sl(*o)
		}
	case *[]uint16:
		if count = r.Uint32(); r.Err == nil {
			*o = make([]uint16, count)
			r.Uint16sl(*o)
		}
	case *[]int32:
		if count = r.Uint32(); r.Err == nil {
			*o = make([]int32, count)
			r.Int32sl(*o)
		}
	case *[]uint32:
		if count = r.Uint32(); r.Err == nil {
			*o = make([]uint32, count)
			r.Uint32sl(*o)
		}
	case *[]int64:
		if count = r.Uint32(); r.Err == nil {
			*o = make([]int64, count)
			r.Int64sl(*o)
		}
	case *[]uint64:
		if count = r.Uint32(); r.Err == nil {
			*o = make([]uint64, count)
			r.Uint64sl(*o)
		}
	case *[]float32:
		if count = r.Uint32(); r.Err == nil {
			*o = make([]float32, count)
			r.Float32sl(*o)
		}
	case *[]float64:
		if count = r.Uint32(); r.Err == nil {
			*o = make([]float64, count)
			r.Float64sl(*o)
		}
	case IReader:
		o.IRead(o, r)
	case []IReader:
		for i := range o {
			o[i].IRead(o[i], r)
		}
	case *[]IReader:
		if count = r.Uint32(); r.Err == nil {
			if int(count) != len(*o) {
				r.Err = fmt.Errorf("Count mismatch: read %d and should %d", count, len(*o))
			} else {
				for i := range *o {
					(*o)[i].IRead((*o)[i], r)
				}
			}
		}
	case IShortReader:
		o.IRead(r)
	case []IShortReader:
		for i := range o {
			o[i].IRead(r)
		}
	case *[]IShortReader:
		if count = r.Uint32(); r.Err == nil {
			if int(count) != len(*o) {
				r.Err = fmt.Errorf("Count mismatch: read %d and should %d", count, len(*o))
			} else {
				for i := range *o {
					(*o)[i].IRead(r)
				}
			}
		}
	case []interface{}:
		for i := range o {
			r.Read(o[i])
		}
	case *[]interface{}:
		if count = r.Uint32(); r.Err == nil {
			if int(count) != len(*o) {
				r.Err = fmt.Errorf("Count mismatch: read %d and should %d", count, len(*o))
			} else {
				for i := range *o {
					r.Read((*o)[i])
				}
			}
		}
	default:
		// Fallback to reflect-based decoding.
		var val reflect.Value
		var ok bool
		if val, ok = i.(reflect.Value); !ok {
			val = reflect.ValueOf(i)
		}
		val = reflect.ValueOf(i)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
			rt := val.Type()
			rd := ReaderFor(rt)
			rd.Auto(r, val)
		} else {
			rt := val.Type()
			rd := ReaderFor(rt)
			rd.Fixed(r, val)
		}
	}
	return r.Err
}

func (r *Reader) ReadValueAuto(val reflect.Value) error {
	if r.Err != nil {
		return r.Err
	}
	rt := val.Type()
	rd := ReaderFor(rt)
	rd.Auto(r, val)
	return r.Err
}

func (r *Reader) ReadValueFixed(val reflect.Value) error {
	if r.Err != nil {
		return r.Err
	}
	rt := val.Type()
	rd := ReaderFor(rt)
	rd.Fixed(r, val)
	return r.Err
}

func (r *Reader) ReadWithSize(i interface{}, rs func(*Reader) int) error {
	if r.Err != nil {
		return r.Err
	}
	switch o := i.(type) {
	case *int8:
		if sz := rs(r); sz == 1 {
			*o = r.Int8()
		}
	case *uint8:
		if sz := rs(r); sz == 1 {
			*o = r.Uint8()
		}
	case *int16:
		if sz := rs(r); sz == 2 {
			*o = r.Int16()
		}
	case *uint16:
		if sz := rs(r); sz == 2 {
			*o = r.Uint16()
		}
	case *int32:
		if sz := rs(r); sz == 4 {
			*o = r.Int32()
		}
	case *uint32:
		if sz := rs(r); sz == 4 {
			*o = r.Uint32()
		}
	case *int64:
		if sz := rs(r); sz == 8 {
			*o = r.Int64()
		}
	case *uint64:
		if sz := rs(r); sz == 8 {
			*o = r.Uint64()
		}
	case *float32:
		if sz := rs(r); sz == 4 {
			*o = r.Float32()
		}
	case *float64:
		if sz := rs(r); sz == 8 {
			*o = r.Float64()
		}
	case []byte:
		if sz := rs(r); sz == len(o) {
			r.Uint8sl(o)
		}
	case *[]byte:
		sz := rs(r)
		*o = r.Slice(sz)
	case *string:
		sz := rs(r)
		*o = r.String(sz)
	default:
		// Fallback to reflect-based decoding.
		val := reflect.ValueOf(i)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
			rt := val.Type()
			rd := ReaderFor(rt)
			rd.Auto(r, val)
		} else {
			rt := val.Type()
			rd := ReaderFor(rt)
			rd.Fixed(r, val)
		}
	}
	return r.Err
}

func (r *Reader) ReadValueWithSize(val reflect.Value, rs func(*Reader) int) error {
	if r.Err != nil {
		return r.Err
	}
	rt := val.Type()
	rd := ReaderFor(rt)
	rd.WithSize(r, val, rs)
	return r.Err
}

func (r *Reader) ReadTail(i interface{}) error {
	if r.Err != nil {
		return r.Err
	}
	was := r.Body
	switch o := i.(type) {
	case *[]int8:
		*o = make([]int8, len(r.Body))
		r.Int8sl(*o)
	case *[]uint8:
		*o = r.Tail()
	case *[]uint16:
		*o = make([]uint16, len(r.Body)/2)
		r.Uint16sl(*o)
	case *[]uint32:
		*o = make([]uint32, len(r.Body)/4)
		r.Uint32sl(*o)
	case *[]uint64:
		*o = make([]uint64, len(r.Body)/8)
		r.Uint64sl(*o)
	case *[]int16:
		*o = make([]int16, len(r.Body)/2)
		r.Int16sl(*o)
	case *[]int32:
		*o = make([]int32, len(r.Body)/4)
		r.Int32sl(*o)
	case *[]int64:
		*o = make([]int64, len(r.Body)/8)
		r.Int64sl(*o)
	case *[]float32:
		*o = make([]float32, len(r.Body)/4)
		r.Float32sl(*o)
	case *[]float64:
		*o = make([]float64, len(r.Body)/8)
		r.Float64sl(*o)
	case IReader:
		o.IRead(o, r)
	case []IReader:
		for i := range o {
			o[i].IRead(o[i], r)
		}
	case IShortReader:
		o.IRead(r)
	case []IShortReader:
		for i := range o {
			o[i].IRead(r)
		}
	case []interface{}:
		for i := range o {
			r.Read(o[i])
		}
	default:
		// Fallback to reflect-based decoding.
		val := reflect.ValueOf(i)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
			rt := val.Type()
			rd := ReaderFor(rt)
			rd.Tail(r, val)
		} else {
			rt := val.Type()
			rd := ReaderFor(rt)
			rd.Fixed(r, val)
		}
	}
	if len(r.Body) != 0 {
		r.Err = fmt.Errorf("Could not read into %+v whole [% x]", i, was)
	}
	return r.Err
}

var rs = make(map[uintptr]*TReader)
var rss = rs
var rsL sync.Mutex

func ReaderFor(rt reflect.Type) (rd *TReader) {
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
		rd = &TReader{Type: rt, Sz: -1, Cnt: -1}
		rss[rtid] = rd
		rd.Fill()
	}
	return
}

type TReader struct {
	Type       reflect.Type
	Implements bool
	Elem       *TReader
	Fixed      func(*Reader, reflect.Value)
	Tail       func(*Reader, reflect.Value)
	Auto       func(*Reader, reflect.Value)
	AutoCount  func(*Reader, reflect.Value, int)
	AutoSize   func(*Reader, reflect.Value, int)
	Sz         int
	SzSet      func(reflect.Value, int) (bool, error)
	Cnt        int
	CntSet     func(reflect.Value, int) error
	Flds       []FieldReader
}

func (t *TReader) SetSize(v reflect.Value, sz int) (bool, error) {
	switch t.Type.Kind() {
	case reflect.Ptr:
		if sz == 0 {
			if !v.IsNil() {
				if v.CanSet() {
					v.Set(reflect.Zero(t.Type))
				} else {
					return t.Elem.SetSize(v.Elem(), sz)
				}
			}
			return true, nil
		} else {
			if v.IsNil() {
				v.Set(reflect.New(t.Elem.Type))
			}
			return t.Elem.SetSize(v.Elem(), sz)
		}
	}
	if t.Sz >= 0 {
		if sz != t.Sz {
			return false, fmt.Errorf("Size doesn't match %d %d %+v", sz, t.Sz, t)
		}
		return true, nil
	} else if t.SzSet != nil {
		return t.SzSet(v, sz)
	} else {
		return false, nil
	}
}

func (t *TReader) WithSize(r *Reader, v reflect.Value, szrd func(*Reader) int) {
	if r.Err != nil {
		return
	}
	if szrd == nil {
		szrd = (*Reader).IntUint32
	}
	sz := szrd(r)
	if t.AutoSize != nil {
		t.AutoSize(r, v, sz)
		return
	}
	if ok, err := t.SetSize(v, sz); ok {
		t.Fixed(r, v)
		return
	} else if err != nil {
		r.Err = err
		return
	}
	rr := Reader{Body: r.Slice(sz), Err: r.Err}
	was := rr.Body
	t.Tail(&rr, v)
	if rr.Err != nil {
		r.Err = rr.Err
	} else if len(rr.Body) != 0 {
		r.Err = fmt.Errorf("Could not read size %d into %+v whole [% x]", sz, t, was)
	}
}

func (t *TReader) SetCount(v reflect.Value, cnt int) error {
	switch t.Type.Kind() {
	case reflect.Ptr:
		if cnt == 0 {
			if !v.IsNil() {
				if v.CanSet() {
					v.Set(reflect.Zero(t.Type))
				} else {
					return t.Elem.SetCount(v.Elem(), cnt)
				}
			}
			return nil
		} else {
			if v.IsNil() {
				/* TODO: give a more meaningful panic message than builtin */
				v.Set(reflect.New(t.Elem.Type))
			}
			return t.Elem.SetCount(v.Elem(), cnt)
		}
	}
	if t.Cnt >= 0 {
		if cnt != t.Cnt {
			return fmt.Errorf("Count doesn't match %d %d %+v", cnt, t.Cnt, t)
		}
	} else if t.CntSet != nil {
		return t.CntSet(v, cnt)
	} else {
		log.Panicf("Don't know what to do with count %+v", t)
	}
	return nil
}

func (t *TReader) WithCount(r *Reader, v reflect.Value, cntrd func(*Reader) int) {
	if r.Err != nil {
		return
	}
	if cntrd == nil {
		cntrd = (*Reader).IntUint32
	}
	cnt := cntrd(r)
	if t.AutoCount != nil {
		t.AutoCount(r, v, cnt)
		return
	}
	if err := t.SetCount(v, cnt); err != nil {
		r.Err = err
		return
	}
	t.Fixed(r, v)
}

func (t *TReader) fillautotail() {
	if t.Fixed == nil && t.AutoCount != nil {
		t.Fixed = func(r *Reader, v reflect.Value) {
			t.WithCount(r, v, (*Reader).IntUint32)
		}
	}
	if t.Tail == nil {
		t.Tail = t.Fixed
	}
	if t.CntSet != nil {
		t.Auto = func(r *Reader, v reflect.Value) {
			t.WithCount(r, v, (*Reader).IntUint32)
		}
	} else {
		t.Auto = t.Fixed
	}
}

func (t *TReader) Fill() {
	defer t.fillautotail()
	rt := t.Type
	if rt.Implements(ireader) {
		t.Implements = true
		t.Fixed = func(r *Reader, v reflect.Value) {
			i := v.Interface()
			i.(IReader).IRead(i, r)
		}
		if rt.Implements(ireadtailer) {
			t.Tail = func(r *Reader, v reflect.Value) {
				i := v.Interface()
				i.(IReadTailer).IReadTail(i, r)
			}
		}
		if rt.Implements(isetsizer) {
			t.SzSet = func(v reflect.Value, s int) (bool, error) {
				i := v.Interface()
				return i.(ISetSizer).ISetSize(i, s)
			}
		}
		if rt.Implements(isetcounter) {
			t.CntSet = func(v reflect.Value, s int) error {
				i := v.Interface()
				return i.(ISetCounter).ISetCount(i, s)
			}
		}
		return
	} else if p := reflect.PtrTo(rt); p.Implements(ireader) {
		t.Implements = true
		t.Fixed = func(r *Reader, v reflect.Value) {
			i := v.Addr().Interface()
			i.(IReader).IRead(i, r)
		}
		if p.Implements(ireadtailer) {
			t.Tail = func(r *Reader, v reflect.Value) {
				i := v.Addr().Interface()
				i.(IReadTailer).IReadTail(i, r)
			}
		}
		if p.Implements(isetsizer) {
			t.SzSet = func(v reflect.Value, s int) (bool, error) {
				i := v.Addr().Interface()
				return i.(ISetSizer).ISetSize(i, s)
			}
		}
		if rt.Implements(isetcounter) {
			t.CntSet = func(v reflect.Value, s int) error {
				i := v.Addr().Interface()
				return i.(ISetCounter).ISetCount(i, s)
			}
		}
		return
	} else if rt.Implements(ishortreader) {
		t.Implements = true
		t.Fixed = func(r *Reader, v reflect.Value) {
			i := v.Interface().(IShortReader)
			i.IRead(r)
		}
		if rt.Implements(ishortreadtailer) {
			t.Tail = func(r *Reader, v reflect.Value) {
				i := v.Interface()
				i.(IShortReadTailer).IReadTail(r)
			}
		}
		if rt.Implements(ishortsetsizer) {
			t.SzSet = func(v reflect.Value, s int) (bool, error) {
				i := v.Interface()
				return i.(IShortSetSizer).ISetSize(s)
			}
		}
		if rt.Implements(ishortsetcounter) {
			t.CntSet = func(v reflect.Value, s int) error {
				i := v.Interface()
				return i.(IShortSetCounter).ISetCount(s)
			}
		}
		return
	} else if p := reflect.PtrTo(rt); p.Implements(ishortreader) {
		t.Implements = true
		t.Fixed = func(r *Reader, v reflect.Value) {
			i := v.Addr().Interface()
			i.(IShortReader).IRead(r)
		}
		if p.Implements(ishortreadtailer) {
			t.Tail = func(r *Reader, v reflect.Value) {
				i := v.Addr().Interface()
				i.(IShortReadTailer).IReadTail(r)
			}
		}
		if p.Implements(ishortsetsizer) {
			t.SzSet = func(v reflect.Value, s int) (bool, error) {
				i := v.Addr().Interface()
				return i.(IShortSetSizer).ISetSize(s)
			}
		}
		if p.Implements(ishortsetcounter) {
			t.CntSet = func(v reflect.Value, s int) error {
				i := v.Addr().Interface()
				return i.(IShortSetCounter).ISetCount(s)
			}
		}
		return
	}

	switch rt.Kind() {
	case reflect.String:
		t.AutoCount = func(r *Reader, v reflect.Value, sz int) {
			v.SetString(r.String(sz))
		}
		t.AutoSize = t.AutoCount
		t.Tail = func(r *Reader, v reflect.Value) {
			v.SetString(string(r.Tail()))
		}
	case reflect.Int8:
		t.Sz = 1
		t.Cnt = 1
		t.Fixed = func(r *Reader, v reflect.Value) {
			v.SetInt(int64(r.Int8()))
		}
	case reflect.Int16:
		t.Sz = 2
		t.Cnt = 1
		t.Fixed = func(r *Reader, v reflect.Value) {
			v.SetInt(int64(r.Int16()))
		}
	case reflect.Int32:
		t.Sz = 4
		t.Cnt = 1
		t.Fixed = func(r *Reader, v reflect.Value) {
			v.SetInt(int64(r.Int32()))
		}
	case reflect.Int64:
		t.Sz = 8
		t.Cnt = 1
		t.Fixed = func(r *Reader, v reflect.Value) {
			v.SetInt(r.Int64())
		}

	case reflect.Uint8:
		t.Sz = 1
		t.Cnt = 1
		t.Fixed = func(r *Reader, v reflect.Value) {
			v.SetUint(uint64(r.Uint8()))
		}
	case reflect.Uint16:
		t.Sz = 2
		t.Cnt = 1
		t.Fixed = func(r *Reader, v reflect.Value) {
			v.SetUint(uint64(r.Uint16()))
		}
	case reflect.Uint32:
		t.Sz = 4
		t.Cnt = 1
		t.Fixed = func(r *Reader, v reflect.Value) {
			v.SetUint(uint64(r.Uint32()))
		}
	case reflect.Uint64:
		t.Sz = 8
		t.Cnt = 1
		t.Fixed = func(r *Reader, v reflect.Value) {
			v.SetUint(r.Uint64())
		}

	case reflect.Float32:
		t.Sz = 4
		t.Cnt = 1
		t.Fixed = func(r *Reader, v reflect.Value) {
			v.SetFloat(float64(r.Float32()))
		}
	case reflect.Float64:
		t.Sz = 8
		t.Cnt = 1
		t.Fixed = func(r *Reader, v reflect.Value) {
			v.SetFloat(r.Float64())
		}
	case reflect.Ptr:
		t.Elem = _reader(rt.Elem())
		t.FillPtr()
	case reflect.Array:
		t.Elem = _reader(rt.Elem())
		t.FillArray()
	case reflect.Slice:
		t.Elem = _reader(rt.Elem())
		t.FillSlice()
	case reflect.Struct:
		t.FillStruct()
	}
	return
}

func (t *TReader) FillPtr() {
	t.Fixed = func(r *Reader, v reflect.Value) {
		if !v.IsNil() {
			t.Elem.Fixed(r, v.Elem())
		}
	}
	t.Tail = func(r *Reader, v reflect.Value) {
		if r.Err == nil {
			return
		}
		if len(r.Body) > 0 {
			if v.IsNil() {
				if v.CanSet() {
					v.Set(reflect.New(t.Elem.Type))
				}
			}
			if !v.IsNil() {
				t.Elem.Tail(r, v.Elem())
			}
		} else if !v.IsNil() {
			if v.CanSet() {
				v.Set(reflect.Zero(t.Type))
			} else {
				/* ah, give up, lets t.Elem.Tail set an error */
				t.Elem.Tail(r, v.Elem())
			}
		}
	}
}

func (t *TReader) FillArray() {
	t.Cnt = t.Type.Len()
	if t.Elem.Sz >= 0 {
		t.Sz = t.Elem.Sz * t.Cnt
	}
	if !t.Elem.Implements {
		switch t.Elem.Type.Kind() {
		case reflect.Uint8:
			t.Fixed = (*Reader).Uint8slVal
			return
		case reflect.Uint16:
			t.Fixed = (*Reader).Uint16slVal
			return
		case reflect.Uint32:
			t.Fixed = (*Reader).Uint32slVal
			return
		case reflect.Uint64:
			t.Fixed = (*Reader).Uint64slVal
			return
		case reflect.Int8:
			t.Fixed = (*Reader).Int8slVal
			return
		case reflect.Int16:
			t.Fixed = (*Reader).Int16slVal
			return
		case reflect.Int32:
			t.Fixed = (*Reader).Int32slVal
			return
		case reflect.Int64:
			t.Fixed = (*Reader).Int64slVal
			return
		case reflect.Float32:
			t.Fixed = (*Reader).Float32slVal
			return
		case reflect.Float64:
			t.Fixed = (*Reader).Float64slVal
			return
		}
	}

	t.Fixed = func(r *Reader, v reflect.Value) {
		for i := 0; i < t.Cnt; i++ {
			t.Elem.Auto(r, v.Index(i))
		}
	}
}

func (t *TReader) FillSlice() {
	t.CntSet = func(v reflect.Value, cnt int) error {
		if v.Len() != cnt {
			v.Set(reflect.MakeSlice(t.Type, cnt, cnt))
		}
		return nil
	}
	if t.Elem.Sz > 0 {
		t.SzSet = func(v reflect.Value, sz int) (bool, error) {
			cnt := sz / t.Elem.Sz
			if v.Len() != cnt {
				v.Set(reflect.MakeSlice(t.Type, cnt, cnt))
			}
			return true, nil
		}
	} else if t.Elem.Sz == 0 {
		t.Sz = 0
	}
	if !t.Elem.Implements {
		switch t.Elem.Type.Kind() {
		case reflect.Uint8:
			if t.Elem.Type == tuint8 {
				t.AutoCount = func(r *Reader, v reflect.Value, sz int) {
					if v.CanSet() {
						v.Set(reflect.ValueOf(r.Slice(sz)))
					} else if sz == v.Len() {
						r.Uint8slVal(v)
					} else {
						r.Err = fmt.Errorf("Could not write to not setable []uint8")
					}
				}
				t.AutoSize = t.AutoCount
			}
			t.Fixed = (*Reader).Uint8slVal
			t.Tail = (*Reader).Uint8slValTail
			return
		case reflect.Uint16:
			t.Fixed = (*Reader).Uint16slVal
			t.Tail = (*Reader).Uint16slValTail
			return
		case reflect.Uint32:
			t.Fixed = (*Reader).Uint32slVal
			t.Tail = (*Reader).Uint32slValTail
			return
		case reflect.Uint64:
			t.Fixed = (*Reader).Uint64slVal
			t.Tail = (*Reader).Uint64slValTail
			return
		case reflect.Int8:
			t.Fixed = (*Reader).Int8slVal
			t.Tail = (*Reader).Int8slValTail
			return
		case reflect.Int16:
			t.Fixed = (*Reader).Int16slVal
			t.Tail = (*Reader).Int16slValTail
			return
		case reflect.Int32:
			t.Fixed = (*Reader).Int32slVal
			t.Tail = (*Reader).Int32slValTail
			return
		case reflect.Int64:
			t.Fixed = (*Reader).Int64slVal
			t.Tail = (*Reader).Int64slValTail
			return
		case reflect.Float32:
			t.Fixed = (*Reader).Float32slVal
			t.Tail = (*Reader).Float32slValTail
			return
		case reflect.Float64:
			t.Fixed = (*Reader).Float64slVal
			t.Tail = (*Reader).Float64slValTail
			return
		}
	}

	t.Fixed = func(r *Reader, v reflect.Value) {
		l := v.Len()
		for i := 0; i < l; i++ {
			t.Elem.Auto(r, v.Index(i))
		}
	}

	t.Tail = func(r *Reader, v reflect.Value) {
		if !v.CanSet() {
			l := v.Len()
			for i := 0; i < l; i++ {
				t.Elem.Auto(r, v.Index(i))
			}
			return
		}
		v.SetLen(0)
		z := reflect.Zero(t.Elem.Type)
		l, c := 0, v.Cap()
		for len(r.Body) > 0 && r.Err == nil {
			l++
			if l <= c {
				v.SetLen(l)
			} else {
				v.Set(reflect.Append(v, z))
				c = v.Cap()
			}
			t.Elem.Auto(r, v.Index(l-1))
		}
	}
}

type FieldReader struct {
	*TReader
	I      int
	NoSize bool
	Tag    reflect.StructTag
	SzRd   func(*Reader) int
	CntRd  func(*Reader) int
}

func (sw *TReader) structFixed(r *Reader, v reflect.Value) {
	for _, fs := range sw.Flds {
		fv := v.Field(fs.I)
		if fs.SzRd != nil {
			fs.WithSize(r, fv, fs.SzRd)
		} else if fs.CntRd != nil {
			fs.WithCount(r, fv, fs.CntRd)
		} else if fs.NoSize {
			fs.Fixed(r, fv)
		} else {
			fs.Auto(r, fv)
		}
	}
}

func (sw *TReader) structTail(r *Reader, v reflect.Value) {
	for _, fs := range sw.Flds {
		fv := v.Field(fs.I)
		if fs.SzRd != nil {
			fs.WithSize(r, fv, fs.SzRd)
		} else if fs.CntRd != nil {
			fs.WithCount(r, fv, fs.CntRd)
		} else if fs.NoSize {
			fs.Tail(r, fv)
		} else {
			fs.Auto(r, fv)
		}
	}
}

func (t *TReader) FillStruct() {
	rt := t.Type
	l := rt.NumField()
	t.Cnt = 1
	size := 0
	for i := 0; i < l; i++ {
		fld := rt.Field(i)
		fr := FieldReader{I: i}
		ipro := fld.Tag.Get("iproto")
		var ber bool

		for _, m := range strings.Split(ipro, ",") {
			if m == "skip" {
				continue
			} else if m == "ber" {
				ber = true
				size = -1
			} else if strings.HasPrefix(m, "size(") {
				if fr.TReader == nil {
					fr.TReader = _reader(fld.Type)
				}

				t := m[5 : len(m)-1]
				switch t {
				case "ber":
					if fr.Sz < 0 {
						size = -1
					} else if size >= 0 {
						size += Varsize(fr.Sz)
					}
					fr.SzRd = (*Reader).Intvar
				case "i8":
					if size >= 0 {
						size += 1
					}
					fr.SzRd = (*Reader).IntUint8
				case "i16":
					if size >= 0 {
						size += 2
					}
					fr.SzRd = (*Reader).IntUint16
				case "i32":
					if size >= 0 {
						size += 4
					}
					fr.SzRd = (*Reader).IntUint32
				case "i64":
					if size >= 0 {
						size += 8
					}
					fr.SzRd = (*Reader).IntUint64
				case "no":
					if fr.I != rt.NumField()-1 {
						log.Panicf("Only last field could be marked as size(no)")
					}
					size = -1
					fr.NoSize = true
				default:
					log.Panicf("Could not understand directive size(%s) for field %s", t, fld.Name)
				}
			} else if strings.HasPrefix(m, "cnt(") {
				if fr.TReader == nil {
					fr.TReader = _reader(fld.Type)
				}

				t := m[4 : len(m)-1]
				switch t {
				case "ber":
					if fr.Cnt < 0 {
						size = -1
					} else if size >= 0 {
						size += Varsize(fr.Cnt)
					}
					fr.CntRd = (*Reader).Intvar
				case "i8":
					if size >= 0 {
						size += 2
					}
					fr.CntRd = (*Reader).IntUint8
				case "i16":
					if size >= 0 {
						size += 2
					}
					fr.CntRd = (*Reader).IntUint16
				case "i32":
					if size >= 0 {
						size += 4
					}
					fr.CntRd = (*Reader).IntUint32
				case "i64":
					if size >= 0 {
						size += 8
					}
					fr.CntRd = (*Reader).IntUint64
				case "no":
					if fr.I != rt.NumField()-1 {
						log.Panicf("Only last field could be marked as cnt(no)")
					}
					size = -1
					fr.NoSize = true
				default:
					log.Panicf("Could not understand directive cnt(%s) for field %s", t, fld.Name)
				}
			}
		}

		if fr.CntRd != nil && fr.SzRd != nil {
			log.Panicf("Sorry, but you shall not use both size() and cnt() iproto tag directive for field %s", fld.Name)
		}

		if fr.TReader == nil {
			fr.TReader = _reader(fld.Type)
		}

		if fr.Sz == 0 && fr.CntRd == nil && fr.SzRd == nil {
			continue
		}

		if size >= 0 {
			size += fr.Sz
		}

		if ber {
			switch fld.Type.Kind() {
			case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				fr.TReader = BerReader
			case reflect.Array:
				switch fld.Type.Elem().Kind() {
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					fr.TReader = &TReader{
						Type: fld.Type,
						Elem: &TReader{},
						Sz:   -1,
						Cnt:  -1,
					}
					*fr.TReader.Elem = *BerReader
					fr.TReader.Elem.Type = fld.Type.Elem()
					fr.TReader.FillArray()
					fr.TReader.fillautotail()
				default:
					log.Panicf("Could not apply 'ber' for array [%d]%+v", fld.Type.Len(), fld.Type.Elem())
				}
			case reflect.Slice:
				switch fld.Type.Elem().Kind() {
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					fr.TReader = &TReader{
						Type: fld.Type,
						Elem: &TReader{},
						Sz:   -1,
						Cnt:  -1,
					}
					*fr.TReader.Elem = *BerReader
					fr.TReader.Elem.Type = fld.Type.Elem()
					fr.TReader.FillSlice()
					fr.TReader.fillautotail()
				default:
					log.Panicf("Could not apply 'ber' for slice []%+v", fld.Type.Elem())
				}
			case reflect.Ptr:
				switch fld.Type.Elem().Kind() {
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					fr.TReader = &TReader{
						Elem: BerReader,
						Sz:   -1,
						Cnt:  -1,
					}
					fr.TReader.FillPtr()
					fr.TReader.fillautotail()
				default:
					log.Panicf("Could not apply 'ber' for ptr *%+v", fld.Type.Elem())
				}
			default:
				log.Panicf("Could not apply 'ber' for type %+v", fld.Type)
			}
		}
		t.Flds = append(t.Flds, fr)
	}
	t.Fixed = t.structFixed
	t.Tail = t.structTail
	t.Sz = size
}

var BerReader = &TReader{
	Type:       tint16,
	Implements: true,
	Elem:       nil,
	Fixed:      (*Reader).Uint64varVal,
	Tail:       (*Reader).Uint64varVal,
	Auto:       (*Reader).Uint64varVal,
	Sz:         -1,
	Cnt:        1,
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
