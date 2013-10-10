package marshal

import (
	"log"
	"reflect"
	"strings"
	"sync"
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

func varsize(i int) (j int) {
	for j = 0; i > 1<<7; j++ {
		i >>= 7
	}
	return j + 1
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

func (w *Writer) Uint8slVal(v reflect.Value) {
	l := v.Len()
	if l > 0 {
		if v.Index(0).CanAddr() {
			p := (*[gg]uint8)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
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
			p := (*[gg]uint8)(unsafe.Pointer(v.Index(0).Addr().Pointer()))
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
		wr := writer(rt)
		wr.WriteAuto(w, val)
	}
	return
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
		tt := _writer(el.Type())
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
		tt := _writer(el.Type())
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

func (t *TWriter) Write_Auto(w *Writer, v reflect.Value) {
	t.WriteAuto(w, v)
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
				el := v.Elem()
				tt := _writer(el.Type())
				tt.Write(w, el)
			},
			WriteAuto: func(w *Writer, v reflect.Value) {
				el := v.Elem()
				tt := _writer(el.Type())
				tt.WriteAuto(w, el)
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
				el := v.Index(i).Elem()
				tt := _writer(el.Type())
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
				el := v.Index(i).Elem()
				tt := _writer(el.Type())
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
	SzWr   func(*Writer, int)
	CntWr  func(*Writer, int)
}

type StructWriter struct {
	Fs []FieldWriter
}

func (sw *StructWriter) writer(w *Writer, v reflect.Value) {
	for _, fs := range sw.Fs {
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
	rt := t.Type
	sw := &StructWriter{}
	l := rt.NumField()
	t.Cnt = 1
	size := 0
	for i := 0; i < l; i++ {
		fld := rt.Field(i)
		fw := FieldWriter{I: i}
		ipro := fld.Tag.Get("iproto")
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

				t := strings.TrimSuffix(strings.TrimPrefix(m, "size("), ")")
				switch t {
				case "ber":
					if fw.Sz < 0 {
						size = -1
					} else if size >= 0 {
						size += varsize(fw.Sz)
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

				t := strings.TrimSuffix(strings.TrimPrefix(m, "cnt("), ")")
				switch t {
				case "ber":
					if fw.Cnt < 0 {
						size = -1
					} else if size >= 0 {
						size += varsize(fw.Cnt)
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
				fw.TWriter = berWriter
			case reflect.Array:
				switch fld.Type.Elem().Kind() {
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					fw.TWriter = &TWriter{}
					*fw.TWriter = *berSlWriter
					fw.TWriter.Type = fld.Type
				default:
					log.Panicf("Could not apply 'ber' for array [%d]%+v", fld.Type.Len(), fld.Type.Elem())
				}
			case reflect.Slice:
				switch fld.Type.Elem().Kind() {
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					fw.TWriter = berSlWriter
				default:
					log.Panicf("Could not apply 'ber' for slice []%+v", fld.Type.Elem())
				}
			case reflect.Ptr:
				switch fld.Type.Elem().Kind() {
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					fw.TWriter = &TWriter{
						Elem: berWriter,
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
		sw.Fs = append(sw.Fs, fw)
	}
	t.Write = sw.writer
	t.Sz = size
}

var berWriter = &TWriter{
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

var berSlWriter = &TWriter{
	Type:       nil,
	Implements: false,
	Elem:       berWriter,
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
	berSlWriter.fillauto()
}
