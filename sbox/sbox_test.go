package sbox

import (
	"bytes"

	"github.com/funny-falcon/go-iproto/marshal"

	//"reflect"
	"testing"
)

type SKey struct {
	Id     int32
	Domain string
}

func TestSelect(t *testing.T) {
	var s SelectReq
	var b, n []byte
	s = SelectReq{
		Space: 3, Index: 2, Offset: 4, Limit: 100,
		Keys: SKey{Id: 1, Domain: "mail.ru"},
	}
	b = []byte{
		3, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 100, 0, 0, 0,
		1, 0, 0, 0,
		2, 0, 0, 0,
		4, 1, 0, 0, 0, 7, 'm', 'a', 'i', 'l', '.', 'r', 'u',
	}
	n = marshal.Write(s)
	if !bytes.Equal(b, n) {
		t.Errorf("Select not match %+v\ngot:\t[% x]\nneed:\t[% x]", s, n, b)
	}
	s = SelectReq{
		Space: 3, Index: 2, Offset: 4, Limit: 100,
		Keys: &SKey{Id: 1, Domain: "mail.ru"},
	}
	n = marshal.Write(s)
	if !bytes.Equal(b, n) {
		t.Errorf("Select not match %+v\ngot:\t[% x]\nneed:\t[% x]", s, n, b)
	}
	s = SelectReq{
		Space: 3, Index: 2, Offset: 4, Limit: 100,
		Keys: []SKey{
			{Id: 1, Domain: "mail.ru"},
			{Id: -1, Domain: "google.com"},
		},
	}
	b = []byte{
		3, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 100, 0, 0, 0,
		2, 0, 0, 0,
		2, 0, 0, 0,
		4, 1, 0, 0, 0, 7, 'm', 'a', 'i', 'l', '.', 'r', 'u',
		2, 0, 0, 0,
		4, 255, 255, 255, 255, 10, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
	}
	n = marshal.Write(s)
	if !bytes.Equal(b, n) {
		t.Errorf("Select not match %+v\ngot:\t[% x]\nneed:\t[% x]", s, n, b)
	}
	s = SelectReq{
		Space: 3, Index: 2, Offset: 4, Limit: 100,
		Keys: &[]SKey{
			{Id: 1, Domain: "mail.ru"},
			{Id: -1, Domain: "google.com"},
		},
	}
	n = marshal.Write(s)
	if !bytes.Equal(b, n) {
		t.Errorf("Select not match %+v\ngot:\t[% x]\nneed:\t[% x]", s, n, b)
	}
	s = SelectReq{
		Space: 3, Index: 2, Offset: 4, Limit: 100,
		Keys: []*SKey{
			&SKey{Id: 1, Domain: "mail.ru"},
			&SKey{Id: -1, Domain: "google.com"},
		},
	}
	n = marshal.Write(s)
	if !bytes.Equal(b, n) {
		t.Errorf("Select not match %+v\ngot:\t[% x]\nneed:\t[% x]", s, n, b)
	}
	s = SelectReq{
		Space: 3, Index: 2, Offset: 4, Limit: 100,
		Keys: []interface{}{
			int32(1),
			[...]int32{1},
			[]int32{-1},
			&SKey{Id: -1, Domain: "google.com"},
		},
	}
	b = []byte{
		3, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 100, 0, 0, 0,
		4, 0, 0, 0,
		1, 0, 0, 0, 4, 1, 0, 0, 0,
		1, 0, 0, 0, 4, 1, 0, 0, 0,
		1, 0, 0, 0, 4, 255, 255, 255, 255,
		2, 0, 0, 0,
		4, 255, 255, 255, 255, 10, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
	}
	n = marshal.Write(s)
	if !bytes.Equal(b, n) {
		t.Errorf("Select not match %+v\ngot:\t[% x]\nneed:\t[% x]", s, n, b)
	}
}
