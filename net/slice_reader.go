package net

import (
	"io"
	"time"
)

type SliceReader struct {
	r        io.Reader
	size     int
	buf      []byte
	timeout  time.Duration
	d        SetDeadliner
	dChecked bool
}

func (sl *SliceReader) Read(n int) (res []byte, err error) {
	if len(sl.buf) > n {
		res = sl.buf[:n]
		sl.buf = sl.buf[n:]
		return
	}
	var buf []byte
	if cap(sl.buf) < n {
		if n > sl.size {
			buf = make([]byte, len(sl.buf), n)
		} else {
			buf = make([]byte, len(sl.buf), sl.size)
		}
		copy(buf, sl.buf)
	} else {
		buf = sl.buf
	}
	l := len(buf)
	for l < n && err == nil {
		var nn int
		nn, err = sl.read(buf[l:cap(buf)])
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
		buf = make([]byte, len(sl.buf), sl.size)
	}
	n, err = sl.read(buf)
	if n >= 1 {
		res = buf[0]
		sl.buf = buf[1:n]
		err = nil
	}
	return
}

func (sl *SliceReader) read(buf []byte) (n int, err error) {
	if sl.timeout > 0 {
		if !sl.dChecked {
			sl.dChecked = true
			sl.d, _ = sl.r.(SetDeadliner)
		}
		if sl.d != nil {
			sl.d.SetReadDeadline(time.Now().Add(sl.timeout))
		}
	}

	if n, err = sl.r.Read(buf); err != nil {
		return
	}

	if sl.timeout > 0 && sl.d != nil {
		sl.d.SetReadDeadline(time.Time{})
	}
	return
}
