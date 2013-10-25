package sbox

import (
	"bytes"
	"github.com/funny-falcon/go-iproto/marshal"
	"reflect"
	"testing"
)

type Should struct {
	v interface{}
	m []byte
}

type SStruct struct {
	I int32
	F byte
	B []byte
	S string
}

type burbur int32

var shoulds = []Should{
	{"asdf", []byte{1, 0, 0, 0, 4, 'a', 's', 'd', 'f'}},
	{[]byte("asdf"), []byte{1, 0, 0, 0, 4, 'a', 's', 'd', 'f'}},
	{int32(0x3def), []byte{1, 0, 0, 0, 4, 0xef, 0x3d, 0, 0}},
	{burbur(0x3def), []byte{1, 0, 0, 0, 4, 0xef, 0x3d, 0, 0}},
	{[]int32{0x3def, 0xff00}, []byte{2, 0, 0, 0, 4, 0xef, 0x3d, 0, 0, 4, 0, 0xff, 0, 0}},
	{&[]int32{0x3def, 0xff00}, []byte{2, 0, 0, 0, 4, 0xef, 0x3d, 0, 0, 4, 0, 0xff, 0, 0}},
	{SStruct{0x3def, 0xfe, []byte{1, 2, 3}, "abcd"},
		[]byte{4, 0, 0, 0, 4, 0xef, 0x3d, 0, 0, 1, 0xfe, 3, 1, 2, 3, 4, 'a', 'b', 'c', 'd'}},
}

var wr = &marshal.Writer{}

func write(v interface{}) []byte {
	WriteTuple(wr, v)
	return wr.Written()
}

func shouldWrite(t *testing.T, v interface{}, m []byte) {
	b := write(v)
	if !bytes.Equal(b, m) {
		t.Errorf("Doesn't match %+v\ngot [% x]\nneed [% x]", v, b, m)
	}
}

func TestWriteTuple(t *testing.T) {
	for _, should := range shoulds {
		shouldWrite(t, should.v, should.m)
	}
}

func read(v interface{}, m []byte) error {
	r := &marshal.Reader{Body: m}
	return ReadRawTuple(r, v)
}

func zerovalue_pointer(v interface{}) interface{} {
	t := reflect.TypeOf(v)
	return reflect.New(t).Interface()
}

func dereference(p interface{}) interface{} {
	v := reflect.ValueOf(p)
	return reflect.Indirect(v).Interface()
}

func should_read(t *testing.T, m []byte, should interface{}) {
	defer func() {
		if err := recover(); err != nil {
			t.Errorf("Fail %v\n:data: [% x]\nshould: %#v", err, m, should)
			panic(err)
		}
	}()
	zero := zerovalue_pointer(should)
	if err := read(zero, m); err != nil {
		t.Errorf("Error %v\ndata: [% x]\nshould: %#v",
			err, m, should)
	} else {
		deref := dereference(zero)
		if dd, ds := dereference(deref), dereference(should); reflect.DeepEqual(dd, ds) {
			return
		}
		t.Errorf("Doesn't match [% x]\ngot: %#v\nshould: %#v",
			m, zero, should)
	}
}

func TestReadTuple(t *testing.T) {
	for _, should := range shoulds {
		//if reflect.TypeOf(should.v).Kind() != reflect.Ptr {
		should_read(t, should.m, should.v)
		//}
	}
}

func TestReadGeneral(t *testing.T) {
	var i, j uint32
	b := []byte{2, 0, 0, 0, 4, 0xff, 0xfe, 0, 0, 4, 0, 0, 0xff, 0xfe}
	if err := read([]interface{}{&i, &j}, b); err != nil {
		t.Error(err)
	}
	if i != 0xfeff || j != 0xfeff0000 {
		t.Errorf("Expected 0xfeff 0xfeff0000, got 0x%x 0x%x", i, j)
	}
	var free [][]byte
	if err := read(&free, b); err != nil {
		t.Error(err)
	}
	if !bytes.Equal(free[0], []byte{0xff, 0xfe, 0, 0}) ||
		!bytes.Equal(free[1], []byte{0, 0, 0xff, 0xfe}) {
		t.Errorf("Expected 0xfeff 0xfeff0000, got 0x%x 0x%x", i, j)
	}
}
