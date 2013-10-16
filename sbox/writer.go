package sbox

import (
	"github.com/funny-falcon/go-iproto/marshal"
	"log"
	"reflect"
	"strings"
	"sync"
)

var _ = log.Print

func WriteTuple(w *marshal.Writer, i interface{}) {
	switch o := i.(type) {
	case nil:
		return
	case uint8:
		w.Uint32(1)
		w.Uint8(1)
		w.Uint8(o)
	case int8:
		w.Uint32(1)
		w.Uint8(1)
		w.Int8(o)
	case uint16:
		w.Uint32(1)
		w.Uint8(2)
		w.Uint16(o)
	case int16:
		w.Uint32(1)
		w.Uint8(2)
		w.Int16(o)
	case uint32:
		w.Uint32(1)
		w.Uint8(4)
		w.Uint32(o)
	case int32:
		w.Uint32(1)
		w.Uint8(4)
		w.Int32(o)
	case uint64:
		w.Uint32(1)
		w.Uint8(8)
		w.Uint64(o)
	case int64:
		w.Uint32(1)
		w.Uint8(8)
		w.Int64(o)
	case float32:
		w.Uint32(1)
		w.Uint8(4)
		w.Float32(o)
	case float64:
		w.Uint32(1)
		w.Uint8(8)
		w.Float64(o)
	case string:
		w.Uint32(1)
		w.Intvar(len(o))
		w.String(o)
	case []byte:
		w.Uint32(1)
		w.Intvar(len(o))
		w.Bytes(o)
	case []interface{}:
		w.IntUint32(len(o))
		for _, v := range o {
			w.WriteWithSize(v, (*marshal.Writer).Intvar)
		}
	default:
		val := reflect.ValueOf(i)
		rt := val.Type()
		wr := writer(rt)
		wr.Write(w, val)
	}
	return
}

var ws = make(map[uintptr]*TWriter)
var wss = ws
var wsL sync.Mutex

func writer(rt reflect.Type) (wr *TWriter) {
	rtid := reflect.ValueOf(rt).Pointer()
	if wr = ws[rtid]; wr == nil {
		wsL.Lock()
		defer wsL.Unlock()
		if wr = ws[rtid]; wr == nil {
			wss = make(map[uintptr]*TWriter, len(ws)+1)
			for t, w := range ws {
				wss[t] = w
			}
			wr = _writer(rt)
			ws = wss
		}
	}
	return
}

func _writer(rt reflect.Type) (wr *TWriter) {
	rtid := reflect.ValueOf(rt).Pointer()
	if wr = wss[rtid]; wr == nil {
		wr = &TWriter{Writer: marshal.WriterFor(rt)}
		wss[rtid] = wr
		wr.Fill()
	}
	return
}

type TWriter struct {
	Writer *marshal.TWriter
	Write  func(*marshal.Writer, reflect.Value)
	Tail   TailType
}

func (t *TWriter) Fill() {
	rt := t.Writer.Type

	switch rt.Kind() {
	case reflect.Ptr:
		elwr := _writer(rt.Elem())
		t.Write = func(w *marshal.Writer, v reflect.Value) {
			if !v.IsNil() {
				elwr.Write(w, v.Elem())
			}
		}
	case reflect.Array:
		t.Write = func(w *marshal.Writer, v reflect.Value) {
			l := v.Len()
			w.IntUint32(l)
			for i := 0; i < l; i++ {
				t.Writer.Elem.WithSize(w, v.Index(i), (*marshal.Writer).Intvar)
			}
		}
	case reflect.Slice:
		t.Write = func(w *marshal.Writer, v reflect.Value) {
			l := v.Len()
			w.IntUint32(l)
			for i := 0; i < l; i++ {
				t.Writer.Elem.WithSize(w, v.Index(i), (*marshal.Writer).Intvar)
			}
		}
	case reflect.Struct:
		t.FillStruct()
	case reflect.Interface:
		t.Write = func(w *marshal.Writer, v reflect.Value) {
			el := v.Elem()
			tt := _writer(el.Type())
			tt.Write(w, el)
		}
	default:
		log.Panicf("Don't know how to write type %+v as a tuple", rt)
	}
	return
}

func (sw *TWriter) structCnt(v reflect.Value) int {
	flds := sw.Writer.Flds
	switch sw.Tail {
	case NoTail:
		return len(flds)
	case Tail:
		return len(flds) - 1 + v.Field(flds[len(flds)-1].I).Len()
	case TailSplit:
		last := flds[len(flds)-1]
		return len(flds) - 1 + v.Field(last.I).Len()*len(last.TWriter.Flds)
	}
	return 0
}

func (sw *TWriter) structWriter(w *marshal.Writer, v reflect.Value) {
	w.IntUint32(sw.structCnt(v))
	flds := sw.Writer.Flds
	n := len(flds)
	if sw.Tail != NoTail {
		n -= 1
	}
	for i := 0; i < n; i++ {
		fs := &flds[i]
		fs.WithSize(w, v.Field(fs.I), (*marshal.Writer).Intvar)
	}
	switch sw.Tail {
	case NoTail:
	case Tail:
		fs := &flds[n]
		fv := v.Field(n)
		l := fv.Len()
		for i := 0; i < l; i++ {
			val := fv.Index(i)
			fs.WithSize(w, val, (*marshal.Writer).Intvar)
		}
	case TailSplit:
		fs := &flds[n]
		fv := v.Field(n)
		l := fv.Len()
		fss := fs.TWriter.Flds
		fl := len(fss)
		for i := 0; i < l; i++ {
			str := fv.Index(i)
			for j := 0; j < fl; j++ {
				fss[i].WithSize(w, str.Field(i), (*marshal.Writer).Intvar)
			}
		}
	}
}

func (t *TWriter) FillStruct() {
	t.Write = t.structWriter
	for i := range t.Writer.Flds {
		fld := &t.Writer.Flds[i]
		ipro := fld.Tag.Get("sbox")

		for _, m := range strings.Split(ipro, ",") {
			if t.Tail != NoTail {
				log.Panicf("Sbox Tail could be only last field in a struct %+v", t.Writer.Type)
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
