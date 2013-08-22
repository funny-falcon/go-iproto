package net

import (
	"io"
	"time"
)


type BufWriter struct {
	w io.Writer
	buf []byte
	wr int
	timeout time.Duration
	d SetDeadliner
	dChecked bool
}

func (w *BufWriter) Write(body []byte) (err error) {
	bl := len(body)
	if w.wr + bl > len(w.buf) {
		if err = w.Flush(); err != nil {
			return err
		}
		if bl > len(w.buf) / 4 {
			return w.write(body)
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
			return
		}
	}

	w.buf[w.wr] = i
	w.wr += 4
	return
}

func (w *BufWriter) Flush() (err error) {
	if w.wr > 0 {
		if err = w.write(w.buf[:w.wr]); err != nil {
			return
		}

		w.wr = 0
		if w.buf == nil {
			w.buf = make([]byte, 4096)
		}
	}
	return
}

func (w *BufWriter) write(buf []byte) (err error) {
	if w.timeout > 0 {
		if !w.dChecked {
			w.dChecked = true
			w.d, _ = w.w.(SetDeadliner)
		}
		if w.d != nil {
			w.d.SetReadDeadline(time.Now().Add(w.timeout))
		}
	}

	if _, err = w.w.Write(buf); err != nil {
		return
	}

	if w.timeout > 0 && w.d != nil {
		w.d.SetWriteDeadline(time.Time{})
	}
	return
}
