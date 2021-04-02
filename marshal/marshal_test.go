package marshal

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

var _ = fmt.Printf
var _ = (*testing.T)(nil)

var printf = fmt.Printf

//var printf = func(...interface{}) {}
type SInts1 struct {
	I8  int8
	I16 int16
	I32 int32
	I64 int64
}
type SInts2 struct {
	U8  uint8
	U16 uint16
	U32 uint32
	U64 uint64
}
type SInts3 struct {
	U8b  uint8  `iproto:"ber"`
	U16b uint16 `iproto:"ber"`
	U32b uint32 `iproto:"ber"`
	U64b uint64 `iproto:"ber"`
}

type SInts4 struct {
	I8sb  int8  `iproto:"size(ber)"`
	I16sb int16 `iproto:"size(ber)"`
	I32sb int32 `iproto:"size(ber)"`
	I64sb int64 `iproto:"size(ber)"`
}

type SInts5 struct {
	U8sb  int8  `iproto:"size(ber)"`
	U16sb int16 `iproto:"size(ber)"`
	U32sb int32 `iproto:"size(ber)"`
	U64sb int64 `iproto:"size(ber)"`
}

type SInts6 struct {
	I8sh  int8  `iproto:"size(i16)"`
	I16sh int16 `iproto:"size(i16)"`
	I32sh int32 `iproto:"size(i16)"`
	I64sh int64 `iproto:"size(i16)"`
}

type SInts7 struct {
	U8sh  int8  `iproto:"size(i16)"`
	U16sh int16 `iproto:"size(i16)"`
	U32sh int32 `iproto:"size(i16)"`
	U64sh int64 `iproto:"size(i16)"`
}

type SInts8 struct {
	I8sl  int8  `iproto:"size(i32)"`
	I16sl int16 `iproto:"size(i32)"`
	I32sl int32 `iproto:"size(i32)"`
	I64sl int64 `iproto:"size(i32)"`
}

type SInts9 struct {
	U8sl  int8  `iproto:"size(i32)"`
	U16sl int16 `iproto:"size(i32)"`
	U32sl int32 `iproto:"size(i32)"`
	U64sl int64 `iproto:"size(i32)"`
}

type SInts10 struct {
	U8slb  uint8  `iproto:"size(i32),ber"`
	U16slb uint16 `iproto:"size(i32),ber"`
	U32slb uint32 `iproto:"size(i32),ber"`
	U64slb uint64 `iproto:"size(i32),ber"`
}

type SInts11 struct {
	U64sbb uint64 `iproto:"size(ber),ber"`
}

type SArr1 struct {
	Ua1 [2]uint32
	Ua2 [2]uint32 `iproto:"size(ber)"`
	Ua3 [2]uint32 `iproto:"size(i16)"`
	Ua4 [2]uint32 `iproto:"cnt(ber)"`
	Ua5 [2]uint32 `iproto:"cnt(i16)"`
	Ua6 [2]uint32 `iproto:"size(ber),ber"`
	Ua7 [2]uint32 `iproto:"cnt(ber),ber"`
}

type SSlice1 struct {
	Ua1 []uint32
	Ua2 []uint32 `iproto:"size(ber)"`
	Ua3 []uint32 `iproto:"size(i16)"`
	Ua4 []uint32 `iproto:"cnt(ber)"`
	Ua5 []uint32 `iproto:"cnt(i16)"`
	Ua6 []uint32 `iproto:"size(ber),ber"`
	Ua7 []uint32 `iproto:"cnt(ber),ber"`
}

type SByte1 struct {
	A byte
	B []byte
	C []int8 `iproto:"size(ber)"`
}

type SString1 struct {
	A string
}

type SS1 struct {
	I int32
	J int16
}
type SS2 struct {
	X int16
	J int32
}
type SStruct1 struct {
	A SS1   `iproto:"cnt(ber)"`
	B []SS2 `iproto:"cnt(i16)"`
}

type SIFace1 struct {
	A interface{}   `iproto:"cnt(i16)"`
	B []interface{} `iproto:"cnt(i16)"`
}

type SIFace2 struct {
	A interface{} `iproto:"size(ber)"`
}

type Should struct {
	v interface{}
	m []byte
}

var shoulds = [...]Should{
	{"abc", []byte{3, 0, 0, 0, 'a', 'b', 'c'}},
	{[...]string{"abc", "cde"}, []byte{3, 0, 0, 0, 'a', 'b', 'c', 3, 0, 0, 0, 'c', 'd', 'e'}},
	{[]string{"abc", "cde"}, []byte{2, 0, 0, 0, 3, 0, 0, 0, 'a', 'b', 'c', 3, 0, 0, 0, 'c', 'd', 'e'}},
	{uint32(1234), []byte{210, 4, 0, 0}},
	{uint32(12345678), []byte{78, 97, 188, 0}},
	{[...]uint32{1234, 12345678}, []byte{210, 4, 0, 0, 78, 97, 188, 0}},
	{&[...]uint32{1234, 12345678}, []byte{210, 4, 0, 0, 78, 97, 188, 0}},
	{[]uint32{1234, 12345678}, []byte{2, 0, 0, 0, 210, 4, 0, 0, 78, 97, 188, 0}},
	{&[]uint32{1234, 12345678}, []byte{2, 0, 0, 0, 210, 4, 0, 0, 78, 97, 188, 0}},
	{SInts1{1, 1, 1, 1}, []byte{1, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0}},
	{&SInts1{1, 1, 1, 1}, []byte{1, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0}},
	{SInts1{-2, -2, -2, -2}, []byte{0xfe, 0xfe, 0xff, 0xfe, 0xff, 0xff, 0xff, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	{SInts2{3, 3, 3, 3}, []byte{3, 3, 0, 3, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0}},
	{SInts3{4, 444, 444444, 4444444444}, []byte{4, 131, 60, 155, 144, 28, 144, 199, 163, 174, 28}},
	{SInts3{5, 5, 5, 5}, []byte{5, 5, 5, 5}},
	{SInts5{6, 6, 6, 6}, []byte{1, 6, 2, 6, 0, 4, 6, 0, 0, 0, 8, 6, 0, 0, 0, 0, 0, 0, 0}},
	{SInts4{-7, -7, -7, -7}, []byte{1, 0xf9, 2, 0xf9, 0xff, 4, 0xf9, 0xff, 0xff, 0xff, 8, 0xf9, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	{SInts6{-8, -8, -8, -8}, []byte{1, 0, 0xf8, 2, 0, 0xf8, 0xff, 4, 0, 0xf8, 0xff, 0xff, 0xff, 8, 0, 0xf8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	{SInts7{9, 9, 9, 9}, []byte{1, 0, 9, 2, 0, 9, 0, 4, 0, 9, 0, 0, 0, 8, 0, 9, 0, 0, 0, 0, 0, 0, 0}},
	{SInts8{-10, -10, -10, -10}, []byte{1, 0, 0, 0, 0xf6, 2, 0, 0, 0, 0xf6, 0xff, 4, 0, 0, 0, 0xf6, 0xff, 0xff, 0xff, 8, 0, 0, 0, 0xf6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	{SInts9{11, 11, 11, 11}, []byte{1, 0, 0, 0, 11, 2, 0, 0, 0, 11, 0, 4, 0, 0, 0, 11, 0, 0, 0, 8, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0}},
	{SInts10{4, 444, 444444, 4444444444}, []byte{1, 0, 0, 0, 4, 2, 0, 0, 0, 131, 60, 3, 0, 0, 0, 155, 144, 28, 5, 0, 0, 0, 144, 199, 163, 174, 28}},
	{[...]SInts11{{4}, {444}, {444444}, {4444444444}}, []byte{1, 4, 2, 131, 60, 3, 155, 144, 28, 5, 144, 199, 163, 174, 28}},
	{[]SInts11{{4}, {444}}, []byte{2, 0, 0, 0, 1, 4, 2, 131, 60}},
	{SArr1{[2]uint32{1, 2}, [2]uint32{3, 4}, [2]uint32{5, 6}, [2]uint32{7, 8}, [2]uint32{9, 10}, [2]uint32{11, 12}, [2]uint32{13, 14}},
		[]byte{
			1, 0, 0, 0, 2, 0, 0, 0,
			8, 3, 0, 0, 0, 4, 0, 0, 0,
			8, 0, 5, 0, 0, 0, 6, 0, 0, 0,
			2, 7, 0, 0, 0, 8, 0, 0, 0,
			2, 0, 9, 0, 0, 0, 10, 0, 0, 0,
			2, 11, 12,
			2, 13, 14,
		}},
	{SSlice1{[]uint32{1, 2}, []uint32{3, 4}, []uint32{5, 6}, []uint32{7, 8}, []uint32{9, 10}, []uint32{11, 12}, []uint32{13, 14}},
		[]byte{
			2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0,
			8, 3, 0, 0, 0, 4, 0, 0, 0,
			8, 0, 5, 0, 0, 0, 6, 0, 0, 0,
			2, 7, 0, 0, 0, 8, 0, 0, 0,
			2, 0, 9, 0, 0, 0, 10, 0, 0, 0,
			2, 11, 12,
			2, 13, 14,
		}},
	{SStruct1{A: SS1{I: 1, J: 2}, B: []SS2{{3, 4}, {5, 6}}},
		[]byte{
			1, 1, 0, 0, 0, 2, 0,
			2, 0, 3, 0, 4, 0, 0, 0, 5, 0, 6, 0, 0, 0,
		}},
	{SString1{A: "asdf"}, []byte{4, 0, 0, 0, 'a', 's', 'd', 'f'}},
	{SByte1{A: 1, B: []byte{1, 2, 3, 4}, C: []int8{2, 3}}, []byte{1, 4, 0, 0, 0, 1, 2, 3, 4, 2, 2, 3}},
}

func should_write(t *testing.T, v interface{}, should []byte) {
	defer func() {
		if err := recover(); err != nil {
			t.Errorf("Fail %v\nvalue: %#v\nshould: [% x]", err, v, should)
			panic(err)
		}
	}()
	encoded := Write(v)
	if !bytes.Equal(encoded, should) {
		t.Errorf("Doesn't match %#v\n% x\n% x", v, encoded, should)
	}
}

func zerovalue_pointer(v interface{}) interface{} {
	t := reflect.TypeOf(v)
	return reflect.New(t).Interface()
}

func dereference(p interface{}) interface{} {
	v := reflect.ValueOf(p)
	return reflect.Indirect(v).Interface()
}

type equal interface {
	Equal(interface{}) bool
}

func should_read(t *testing.T, m []byte, should interface{}) {
	defer func() {
		if err := recover(); err != nil {
			t.Errorf("Fail %v\n:data: [% x]\nshould: %#v", err, m, should)
			panic(err)
		}
	}()
	zero := zerovalue_pointer(should)
	if err := Read(m, zero); err != nil {
		t.Errorf("Error %v\ndata: [% x]\nshould: %#v",
			err, m, should)
	} else {
		deref := dereference(zero)
		if e, ok := should.(equal); ok {
			if e.Equal(deref) {
				return
			}
		} else if dd, ds := dereference(deref), dereference(should); reflect.DeepEqual(dd, ds) {
			return
		}
		t.Errorf("Doesn't match [% x]\ngot: %#v\nshould: %#v",
			m, zero, should)
	}
}

func TestEncode(t *testing.T) {
	for _, should := range shoulds {
		should_write(t, should.v, should.m)
	}
}

func TestDecode(t *testing.T) {
	for _, should := range shoulds {
		if reflect.TypeOf(should.v).Kind() != reflect.Ptr {
			should_read(t, should.m, should.v)
		}
	}
}

func TestEncodeSIFace(t *testing.T) {
	s := SIFace1{
		A: []SInts11{{4}, {444}, {444444}, {4444444444}},
		B: []interface{}{SInts1{1, 1, 1, 1}, SInts3{4, 444, 444444, 4444444444}},
	}
	n := []byte{4, 0, 1, 4, 2, 131, 60, 3, 155, 144, 28, 5, 144, 199, 163, 174, 28,
		2, 0,
		1, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
		4, 131, 60, 155, 144, 28, 144, 199, 163, 174, 28}
	should_write(t, s, n)
	s = SIFace1{
		A: SInts1{1, 1, 1, 1},
		B: nil,
	}
	n = []byte{1, 0, 1, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	should_write(t, s, n)

	ss := SIFace2{int32(1)}
	n = []byte{4, 1, 0, 0, 0}
	should_write(t, ss, n)
	ss = SIFace2{}
	n = []byte{0}
	should_write(t, ss, n)
}

var ballast = make([]byte, 0, 100000000)

func BenchmarkEncode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, should := range shoulds {
			Write(should.v)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, should := range shoulds {
			if reflect.TypeOf(should.v).Kind() != reflect.Ptr {
				zero := zerovalue_pointer(should.v)
				Read(should.m, zero)
			}
		}
	}
}
