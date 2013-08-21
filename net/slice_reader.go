package net

import (
	"io"
)

type SliceReader struct {
	reader io.Reader
	Size int
	buf []byte
}

func (sl *SliceReader) Read(n int) (res []byte, err error) {
	if len(sl.buf) > n {
		res = sl.buf[:n]
		sl.buf = sl.buf[n:]
		return
	}
	var buf []byte
	if cap(sl.buf) < n {
		if n > sl.Size {
			buf = make([]byte, len(sl.buf), n)
		} else {
			buf = make([]byte, len(sl.buf), sl.Size)
		}
		copy(buf, sl.buf)
	} else {
		buf = sl.buf
	}
	l := len(buf)
	for l < n && err == nil {
		var nn int
		nn, err = sl.reader.Read(buf[l:cap(buf)])
		l += nn
	}
	if l >= n {
		res = buf[:n]
		sl.buf = buf[n:l]
		err = nil
	} else if l > 0 && err == io.EOF {
		res = buf[:l]
		sl.buf = buf[l:]
		err = io.ErrUnexpectedEOF
	}
	return
}

func (sl *SliceReader) ReadByte() (res byte, err error) {
	if len(sl.buf) > 1 {
		res = sl.buf[0]
		sl.buf = sl.buf[1:]
		return
	}
	var buf []byte
	var n int
	if cap(buf) == 0 {
		buf = make([]byte, len(sl.buf), sl.Size)
	}
	n, err = sl.reader.Read(buf)
	if n >= 1 {
		res = buf[0]
		sl.buf = buf[1:n]
		err = nil
	}
	return
}
