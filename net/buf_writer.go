package net

import (
	"io"
)


type BufWriter struct {
	w io.Writer
	buf []byte
	wr int
}

func (w *BufWriter) Write(body []byte) (err error) {
	bl := len(body)
	if w.wr + bl > len(w.buf) {
		if err = w.Flush(); err != nil {
			return err
		}
		if bl > len(w.buf) / 4 {
			_, err = w.w.Write(body)
			return
		}
	}
	copy(w.buf[w.wr:w.wr+bl], body)
	w.wr += bl
	return
}

func (w *BufWriter) WriteUint32(i uint32) (err error) {
	if w.wr + 4 > len(w.buf) {
		if err = w.Flush(); err != nil {
			return err
		}
	}

	bin_le.PutUint32(w.buf[w.wr:w.wr+4], i)
	w.wr += 4
	return
}

func (w *BufWriter) WriteByte(i byte) (err error) {
	if w.wr + 4 > len(w.buf) {
		if err = w.Flush(); err != nil {
			return err
		}
	}

	w.buf[w.wr] = i
	w.wr += 4
	return
}

func (w *BufWriter) Flush() (err error) {
	if w.wr > 0 {
		if _, err = w.w.Write(w.buf[:w.wr]); err != nil {
			return
		}
		w.wr = 0
		if w.buf == nil {
			w.buf = make([]byte, 4096)
		}
	}
	return
}
