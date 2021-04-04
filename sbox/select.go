package sbox

import (
	"log"
	"reflect"

	"github.com/funny-falcon/go-iproto"
	"github.com/funny-falcon/go-iproto/marshal"
)

var _ = log.Printf

const (
	SelectAll = int32(-1)
	SelectN   = int32(-2)
)

type SelectReq struct {
	Space, Index uint32
	Offset       uint32
	Limit        int32

	Keys interface{}
}

func (s SelectReq) IWrite(w *marshal.Writer) {
	w.Uint32(s.Space)
	w.Uint32(s.Index)
	w.Uint32(s.Offset)
	cnt := CountKeys(s.Keys)
	if s.Limit >= 0 {
		w.Int32(s.Limit)
	} else if s.Limit == SelectN {
		w.Int32(int32(cnt))
	} else {
		w.Int32(1<<31 - 1)
	}
	w.IntUint32(cnt)
	WriteKeys(w, s.Keys)
}

func (s SelectReq) IMsg() iproto.RequestType {
	return 17
}

func CountKeys(keys interface{}) int {
	switch k := keys.(type) {
	case int8, uint8, int16, uint16, int32, uint32, int64, uint64, []byte, string:
		return 1
	case []uint32:
		return len(k)
	case []int32:
		return len(k)
	case []uint64:
		return len(k)
	case []int64:
		return len(k)
	case []float32:
		return len(k)
	case []float64:
		return len(k)
	case []string:
		return len(k)
	case [][]byte:
		return len(k)
	case [][][]byte:
		return len(k)
	case []interface{}:
		sum := 0
		for _, v := range k {
			sum += CountKeys(v)
		}
		return sum
	default:
		v := reflect.ValueOf(k)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		return CountKeysValues(v)
	}
}

func CountKeysValues(v reflect.Value) int {
	switch v.Kind() {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64,
		reflect.Struct, reflect.String:
		return 1
	case reflect.Array, reflect.Slice:
		if v.Kind() == reflect.Uint8 {
			return 1
		}
		return v.Len()
	default:
		log.Panicf("Don't know how to use value %+v as select keys", v)
	}
	return 0
}

func WriteKeys(w *marshal.Writer, keys interface{}) {
	switch k := keys.(type) {
	case int8, uint8, int16, uint16, int32, uint32, int64, uint64, string, []byte:
		WriteTuple(w, k)
	case []uint32:
		for _, v := range k {
			WriteTuple(w, v)
		}
	case []int32:
		for _, v := range k {
			WriteTuple(w, v)
		}
	case []uint64:
		for _, v := range k {
			WriteTuple(w, v)
		}
	case []int64:
		for _, v := range k {
			WriteTuple(w, v)
		}
	case []float32:
		for _, v := range k {
			WriteTuple(w, v)
		}
	case []float64:
		for _, v := range k {
			WriteTuple(w, v)
		}
	case []string:
		for _, v := range k {
			WriteTuple(w, v)
		}
	case [][]byte:
		for _, v := range k {
			WriteTuple(w, v)
		}
	case [][][]byte:
		for _, v := range k {
			WriteTuple(w, v)
		}
	case []interface{}:
		for _, v := range k {
			WriteTuple(w, v)
		}
	default:
		v := reflect.ValueOf(k)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		WriteKeysValues(w, v)
	}
}

func WriteKeysValues(w *marshal.Writer, v reflect.Value) {
	switch v.Kind() {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64,
		reflect.Struct, reflect.String:
		wr := writer(v.Type())
		wr.Write(w, v)
	case reflect.Array, reflect.Slice:
		l := v.Len()
		switch v.Kind() {
		case reflect.Uint8:
			wr := writer(v.Type())
			wr.Write(w, v)
		case reflect.Struct, reflect.Array, reflect.Slice,
			reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Float32, reflect.Float64, reflect.Ptr:
			wr := writer(v.Type().Elem())
			for i := 0; i < l; i++ {
				wr.Write(w, v.Index(i))
			}
		case reflect.Interface:
			for i := 0; i < l; i++ {
				val := v.Index(i)
				wr := writer(val.Type())
				wr.Write(w, val)
			}
		}
	}
}
