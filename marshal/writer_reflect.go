package marshal

import (
	"log"
	"reflect"
	"strings"
	"sync"
)

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
	case string:
		w.IntUint32(len(o))
		w.String(o)
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
		w.IntUint32(len(o))
		for _, v := range o {
			v.IWrite(v, w)
		}
	case IShortWriter:
		o.IWrite(w)
	case []IShortWriter:
		w.IntUint32(len(o))
		for _, v := range o {
			v.IWrite(w)
		}
	case []interface{}:
		w.IntUint32(len(o))
		for _, v := range o {
			w.Write(v)
		}
	default:
		val := reflect.ValueOf(i)
		rt := val.Type()
		wr := WriterFor(rt)
		wr.WriteAuto(w, val)
	}
	return
}

func (w *Writer) WriteValue(val reflect.Value) {
	rt := val.Type()
	wr := WriterFor(rt)
	wr.WriteAuto(w, val)
}

func (w *Writer) WriteTail(i interface{}) {
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
	case IWriter:
		o.IWrite(o, w)
	case []IWriter:
		for _, v := range o {
			v.IWrite(v, w)
		}
	case IShortWriter:
		o.IWrite(w)
	case []IShortWriter:
		for _, v := range o {
			v.IWrite(w)
		}
	case []interface{}:
		for _, v := range o {
			w.Write(v)
		}
	default:
		val := reflect.ValueOf(i)
		rt := val.Type()
		wr := WriterFor(rt)
		wr.Write(w, val)
	}
	return
}

func (w *Writer) WriteWithSize(i interface{}, ws func(*Writer, int)) {
	switch o := i.(type) {
	case nil:
		return
	case uint8:
		ws(w, 1)
		w.Uint8(o)
	case int8:
		ws(w, 1)
		w.Int8(o)
	case uint16:
		ws(w, 2)
		w.Uint16(o)
	case int16:
		ws(w, 2)
		w.Int16(o)
	case uint32:
		ws(w, 4)
		w.Uint32(o)
	case int32:
		ws(w, 4)
		w.Int32(o)
	case uint64:
		ws(w, 8)
		w.Uint64(o)
	case int64:
		ws(w, 8)
		w.Int64(o)
	case float32:
		ws(w, 4)
		w.Float32(o)
	case float64:
		ws(w, 8)
		w.Float64(o)
	case string:
		ws(w, len(o))
		w.String(o)
	case []uint8:
		ws(w, len(o))
		w.Uint8sl(o)
	default:
		val := reflect.ValueOf(i)
		rt := val.Type()
		wr := WriterFor(rt)
		wr.WithSize(w, val, ws)
	}
	return
}

func (w *Writer) WriteValueWithSize(val reflect.Value, ws func(*Writer, int)) {
	rt := val.Type()
	wr := WriterFor(rt)
	wr.WithSize(w, val, ws)
}

var ws = make(map[uintptr]*TWriter)
var wss = ws
var wsL sync.Mutex

func WriterFor(rt reflect.Type) (wr *TWriter) {
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
		wr = &TWriter{Type: rt, Sz: -1, Cnt: -1}
		wss[rtid] = wr
		wr.Fill()
	}
	return
}

type TWriter struct {
	Type       reflect.Type
	Implements bool
	Elem       *TWriter
	Write      func(*Writer, reflect.Value)
	WriteAuto  func(*Writer, reflect.Value)
	Sz         int
	SzGet      func(reflect.Value) int
	Cnt        int
	CntGet     func(reflect.Value) int
	Flds       []FieldWriter
}

var twriters = make(chan *Writer, 512)

func puttwriter(w *Writer) {
	select {
	case twriters <- w:
	default:
	}
}

func (t *TWriter) Size(v reflect.Value) int {
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return 0
		}
		t = t.Elem
		v = v.Elem()
	case reflect.Interface:
		if v.IsNil() {
			return 0
		}
		el := v.Elem()
		tt := WriterFor(el.Type())
		return tt.Size(el)
	}
	if t.Sz >= 0 {
		return t.Sz
	} else if t.SzGet != nil {
		return t.SzGet(v)
	} else {
		return -1
	}
}

func (t *TWriter) WithSize(w *Writer, v reflect.Value, szwr func(*Writer, int)) {
	sz := t.Size(v)
	if szwr == nil {
		szwr = (*Writer).IntUint32
	}
	if sz >= 0 {
		szwr(w, sz)
		t.Write(w, v)
	} else {
		var tw *Writer
		select {
		case tw = <-twriters:
		default:
			tw = &Writer{DefSize: 128}
		}
		t.Write(tw, v)
		body := tw.Written()
		szwr(w, len(body))
		w.Bytes(body)
		puttwriter(tw)
	}
}

func (t *TWriter) Count(v reflect.Value) int {
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return 0
		}
		t = t.Elem
		v = v.Elem()
	case reflect.Interface:
		if v.IsNil() {
			return 0
		}
		el := v.Elem()
		tt := WriterFor(el.Type())
		return tt.Count(el)
	}
	if t.Cnt >= 0 {
		return t.Cnt
	} else if t.CntGet != nil {
		if cnt := t.CntGet(v); cnt >= 0 {
			return cnt
		}
	}
	log.Panicf("could not determine count %+v %+v", t, v.Interface())
	return -1
}

func (t *TWriter) WithCount(w *Writer, v reflect.Value, cntwr func(*Writer, int)) {
	cnt := t.Count(v)
	if cntwr == nil {
		cntwr = (*Writer).IntUint32
	}
	cntwr(w, cnt)
	t.Write(w, v)
}

func (t *TWriter) fillauto() {
	if t.WriteAuto == nil {
		if t.CntGet != nil {
			t.WriteAuto = func(w *Writer, v reflect.Value) {
				t.WithCount(w, v, (*Writer).IntUint32)
			}
		} else {
			t.WriteAuto = t.Write
		}
	}
}

func (t *TWriter) Fill() {
	defer t.fillauto()
	rt := t.Type
	if rt.Implements(iwriter) {
		t.Implements = true
		t.Write = func(w *Writer, v reflect.Value) {
			i := v.Interface()
			i.(IWriter).IWrite(i, w)
		}
		if rt.Implements(isizer) {
			t.SzGet = func(v reflect.Value) int {
				i := v.Interface()
				return i.(ISizer).ISize(i)
			}
		}
		if rt.Implements(icounter) {
			t.CntGet = func(v reflect.Value) int {
				i := v.Interface()
				return i.(ICounter).ICount(i)
			}
		}
		return
	} else if p := reflect.PtrTo(rt); p.Implements(iwriter) {
		t.Implements = true
		t.Write = func(w *Writer, v reflect.Value) {
			i := v.Addr().Interface()
			i.(IWriter).IWrite(i, w)
		}
		if p.Implements(isizer) {
			t.SzGet = func(v reflect.Value) int {
				i := v.Addr().Interface()
				return i.(ISizer).ISize(i)
			}
		}
		if p.Implements(icounter) {
			t.CntGet = func(v reflect.Value) int {
				i := v.Addr().Interface()
				return i.(ICounter).ICount(i)
			}
		}
		return
	} else if rt.Implements(ishortwriter) {
		t.Implements = true
		t.Write = func(w *Writer, v reflect.Value) {
			v.Interface().(IShortWriter).IWrite(w)
		}
		if rt.Implements(ishortsizer) {
			t.SzGet = func(v reflect.Value) int {
				return v.Interface().(IShortSizer).ISize()
			}
		}
		if rt.Implements(ishortcounter) {
			t.CntGet = func(v reflect.Value) int {
				i := v.Interface()
				return i.(IShortCounter).ICount()
			}
		}
		return
	} else if p := reflect.PtrTo(rt); p.Implements(ishortwriter) {
		t.Implements = true
		t.Write = func(w *Writer, v reflect.Value) {
			v.Addr().Interface().(IShortWriter).IWrite(w)
		}
		if p.Implements(ishortsizer) {
			t.SzGet = func(v reflect.Value) int {
				return v.Addr().Interface().(IShortSizer).ISize()
			}
		}
		if p.Implements(ishortcounter) {
			t.CntGet = func(v reflect.Value) int {
				i := v.Addr().Interface()
				return i.(IShortCounter).ICount()
			}
		}
		return
	}

	switch rt.Kind() {
	case reflect.String:
		t.Write = (*Writer).StringVal
		t.SzGet = reflect.Value.Len
		t.CntGet = reflect.Value.Len
	case reflect.Int8:
		t.Write = (*Writer).Int8Val
		t.Sz = 1
		t.Cnt = 1
	case reflect.Int16:
		t.Write = (*Writer).Int16Val
		t.Sz = 2
		t.Cnt = 1
	case reflect.Int32:
		t.Write = (*Writer).Int32Val
		t.Sz = 4
		t.Cnt = 1
	case reflect.Int64:
		t.Write = (*Writer).Int64Val
		t.Sz = 8
		t.Cnt = 1
	case reflect.Uint8:
		t.Write = (*Writer).Uint8Val
		t.Sz = 1
		t.Cnt = 1
	case reflect.Uint16:
		t.Write = (*Writer).Uint16Val
		t.Sz = 2
		t.Cnt = 1
	case reflect.Uint32:
		t.Write = (*Writer).Uint32Val
		t.Sz = 4
		t.Cnt = 1
	case reflect.Uint64:
		t.Write = (*Writer).Uint64Val
		t.Sz = 8
		t.Cnt = 1
	case reflect.Float32:
		t.Write = (*Writer).Float32Val
		t.Sz = 4
		t.Cnt = 1
	case reflect.Float64:
		t.Write = (*Writer).Float64Val
		t.Sz = 8
		t.Cnt = 1
	case reflect.Ptr:
		t.Elem = _writer(rt.Elem())
		t.FillPtr()
	case reflect.Array:
		t.Elem = _writer(rt.Elem())
		t.FillArray()
	case reflect.Slice:
		t.Elem = _writer(rt.Elem())
		t.FillSlice()
	case reflect.Struct:
		t.FillStruct()
	case reflect.Interface:
		*t = TWriter{
			Type: reflect.TypeOf(new(interface{})).Elem(),
			Elem: nil,
			Write: func(w *Writer, v reflect.Value) {
				if !v.IsNil() {
					el := v.Elem()
					tt := WriterFor(el.Type())
					tt.Write(w, el)
				}
			},
			WriteAuto: func(w *Writer, v reflect.Value) {
				if !v.IsNil() {
					el := v.Elem()
					tt := WriterFor(el.Type())
					tt.WriteAuto(w, el)
				}
			},
			Sz:  -1,
			Cnt: -1,
		}
	}
	return
}

func (t *TWriter) FillArray() {
	t.Cnt = t.Type.Len()
	if t.Elem.Sz >= 0 {
		t.Sz = t.Cnt * t.Elem.Sz
	}
	if !t.Elem.Implements {
		switch t.Elem.Type.Kind() {
		case reflect.Uint8:
			t.Write = (*Writer).Uint8slVal
			return
		case reflect.Uint16:
			t.Write = (*Writer).Uint16slVal
			return
		case reflect.Uint32:
			t.Write = (*Writer).Uint32slVal
			return
		case reflect.Uint64:
			t.Write = (*Writer).Uint64slVal
			return
		case reflect.Int8:
			t.Write = (*Writer).Int8slVal
			return
		case reflect.Int16:
			t.Write = (*Writer).Int16slVal
			return
		case reflect.Int32:
			t.Write = (*Writer).Int32slVal
			return
		case reflect.Int64:
			t.Write = (*Writer).Int64slVal
			return
		case reflect.Float32:
			t.Write = (*Writer).Float32slVal
			return
		case reflect.Float64:
			t.Write = (*Writer).Float64slVal
			return
		}
	}

	if t.Elem.Type.Kind() != reflect.Interface {
		t.Write = func(w *Writer, v reflect.Value) {
			for i := 0; i < t.Cnt; i++ {
				t.Elem.WriteAuto(w, v.Index(i))
			}
		}
	} else {
		t.Write = func(w *Writer, v reflect.Value) {
			l := v.Len()
			for i := 0; i < l; i++ {
				if v.Index(i).IsNil() {
					continue
				}
				el := v.Index(i).Elem()
				tt := WriterFor(el.Type())
				tt.Write(w, el)
			}
		}
	}
}

func (t *TWriter) FillSlice() {
	t.CntGet = reflect.Value.Len
	if t.Elem.Sz >= 0 {
		t.SzGet = func(v reflect.Value) int { return v.Len() * t.Elem.Sz }
	}
	if !t.Elem.Implements {
		switch t.Elem.Type.Kind() {
		case reflect.Uint8:
			t.Write = (*Writer).Uint8slVal
			return
		case reflect.Uint16:
			t.Write = (*Writer).Uint16slVal
			return
		case reflect.Uint32:
			t.Write = (*Writer).Uint32slVal
			return
		case reflect.Uint64:
			t.Write = (*Writer).Uint64slVal
			return
		case reflect.Int8:
			t.Write = (*Writer).Int8slVal
			return
		case reflect.Int16:
			t.Write = (*Writer).Int16slVal
			return
		case reflect.Int32:
			t.Write = (*Writer).Int32slVal
			return
		case reflect.Int64:
			t.Write = (*Writer).Int64slVal
			return
		case reflect.Float32:
			t.Write = (*Writer).Float32slVal
			return
		case reflect.Float64:
			t.Write = (*Writer).Float64slVal
			return
		}
	}
	if t.Elem.Type.Kind() != reflect.Interface {
		t.Write = func(w *Writer, v reflect.Value) {
			l := v.Len()
			for i := 0; i < l; i++ {
				t.Elem.WriteAuto(w, v.Index(i))
			}
		}
	} else {
		t.Write = func(w *Writer, v reflect.Value) {
			l := v.Len()
			for i := 0; i < l; i++ {
				if v.Index(i).IsNil() {
					continue
				}
				el := v.Index(i).Elem()
				tt := WriterFor(el.Type())
				tt.Write(w, el)
			}
		}
	}
}

func (t *TWriter) FillPtr() {
	t.Write = func(w *Writer, v reflect.Value) {
		if !v.IsNil() {
			t.Elem.Write(w, v.Elem())
		}
	}
	t.WriteAuto = func(w *Writer, v reflect.Value) {
		t.Elem.WriteAuto(w, v.Elem())
	}
}

type FieldWriter struct {
	*TWriter
	I      int
	NoSize bool
	Tag    reflect.StructTag
	SzWr   func(*Writer, int)
	CntWr  func(*Writer, int)
}

func (t *TWriter) writeStruct(w *Writer, v reflect.Value) {
	for _, fs := range t.Flds {
		fv := v.Field(fs.I)
		if fs.SzWr != nil {
			fs.WithSize(w, fv, fs.SzWr)
		} else if fs.CntWr != nil {
			fs.WithCount(w, fv, fs.CntWr)
		} else if fs.NoSize {
			fs.Write(w, fv)
		} else {
			fs.WriteAuto(w, fv)
		}
	}
}

func (t *TWriter) FillStruct() {
	t.Cnt = 1
	rt := t.Type
	l := rt.NumField()
	size := 0
	for i := 0; i < l; i++ {
		fld := rt.Field(i)
		ipro := fld.Tag.Get("iproto")
		fw := FieldWriter{I: i, Tag: fld.Tag}
		var ber bool

		for _, m := range strings.Split(ipro, ",") {
			if m == "skip" {
				continue
			} else if m == "ber" {
				ber = true
				size = -1
			} else if strings.HasPrefix(m, "size(") {
				if fw.TWriter == nil {
					fw.TWriter = _writer(fld.Type)
				}

				t := m[5 : len(m)-1]
				switch t {
				case "ber":
					if fw.Sz < 0 {
						size = -1
					} else if size >= 0 {
						size += Varsize(fw.Sz)
					}
					fw.SzWr = (*Writer).Intvar
				case "i8":
					if size >= 0 {
						size += 1
					}
					fw.SzWr = (*Writer).IntUint8
				case "i16":
					if size >= 0 {
						size += 2
					}
					fw.SzWr = (*Writer).IntUint16
				case "i32":
					if size >= 0 {
						size += 4
					}
					fw.SzWr = (*Writer).IntUint32
				case "i64":
					if size >= 0 {
						size += 8
					}
					fw.SzWr = (*Writer).IntUint64
				case "no":
					if fw.I != rt.NumField()-1 {
						log.Panicf("Only last field could be marked as size(no)")
					}
					size = -1
					fw.NoSize = true
				default:
					log.Panicf("Could not understand directive size(%s) for field %s", t, fld.Name)
				}
			} else if strings.HasPrefix(m, "cnt(") {
				if fw.TWriter == nil {
					fw.TWriter = _writer(fld.Type)
				}

				t := m[4 : len(m)-1]
				switch t {
				case "ber":
					if fw.Cnt < 0 {
						size = -1
					} else if size >= 0 {
						size += Varsize(fw.Cnt)
					}
					fw.CntWr = (*Writer).Intvar
				case "i8":
					if size >= 0 {
						size += 2
					}
					fw.CntWr = (*Writer).IntUint8
				case "i16":
					if size >= 0 {
						size += 2
					}
					fw.CntWr = (*Writer).IntUint16
				case "i32":
					if size >= 0 {
						size += 4
					}
					fw.CntWr = (*Writer).IntUint32
				case "i64":
					if size >= 0 {
						size += 8
					}
					fw.CntWr = (*Writer).IntUint64
				case "no":
					if fw.I != rt.NumField()-1 {
						log.Panicf("Only last field could be marked as cnt(no)")
					}
					size = -1
					fw.NoSize = true
				default:
					log.Panicf("Could not understand directive cnt(%s) for field %s", t, fld.Name)
				}
			}
		}

		if fw.CntWr != nil && fw.SzWr != nil {
			log.Panicf("Sorry, but you shall not use both size() and cnt() iproto tag directive for field %s", fld.Name)
		}

		if fw.TWriter == nil {
			fw.TWriter = _writer(fld.Type)
		}

		if fw.Sz == 0 && fw.CntWr == nil && fw.SzWr == nil {
			continue
		}

		if size >= 0 {
			size += fw.Sz
		}

		if ber {
			switch fld.Type.Kind() {
			case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				fw.TWriter = BerWriter
			case reflect.Array:
				switch fld.Type.Elem().Kind() {
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					fw.TWriter = &TWriter{}
					*fw.TWriter = *BerSlWriter
					fw.TWriter.Type = fld.Type
				default:
					log.Panicf("Could not apply 'ber' for array [%d]%+v", fld.Type.Len(), fld.Type.Elem())
				}
			case reflect.Slice:
				switch fld.Type.Elem().Kind() {
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					fw.TWriter = BerSlWriter
				default:
					log.Panicf("Could not apply 'ber' for slice []%+v", fld.Type.Elem())
				}
			case reflect.Ptr:
				switch fld.Type.Elem().Kind() {
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					fw.TWriter = &TWriter{
						Elem: BerWriter,
						Sz:   -1,
						Cnt:  -1,
					}
					fw.TWriter.FillPtr()
				default:
					log.Panicf("Could not apply 'ber' for ptr *%+v", fld.Type.Elem())
				}
			default:
				log.Panicf("Could not apply 'ber' for type %+v", fld.Type)
			}
		}
		t.Flds = append(t.Flds, fw)
	}
	t.Write = t.writeStruct
	t.Sz = size
}

var BerWriter = &TWriter{
	Type:       tint16,
	Implements: true,
	Write:      (*Writer).VarVal,
	WriteAuto:  (*Writer).VarVal,
	Sz:         -1,
	SzGet: func(v reflect.Value) int {
		return varu64size(v.Uint())
	},
	Cnt: 1,
}

var BerSlWriter = &TWriter{
	Type:       nil,
	Implements: false,
	Elem:       BerWriter,
	Write:      (*Writer).VarslVal,
	Sz:         -1,
	SzGet: func(v reflect.Value) int {
		l := v.Len()
		s := 0
		for i := 0; i < l; i++ {
			s += varu64size(v.Index(i).Uint())
		}
		return s
	},
	Cnt:    -1,
	CntGet: reflect.Value.Len,
}

func init() {
	BerSlWriter.fillauto()
}
