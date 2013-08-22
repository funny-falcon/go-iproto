package net

import (
	"github.com/funny-falcon/go-iproto"
	"io"
	"encoding/binary"
	"time"
)

var bin_le = binary.LittleEndian

type Request struct {
	Msg      iproto.RequestType
	Body     []byte
	Id       uint32
}

type Response iproto.Response

type HeaderReader struct {
	r  SliceReader
}

func (h *HeaderReader) Init(conn io.Reader, timeout time.Duration) {
	h.r = SliceReader{ r: conn, size: 16*1024, timeout: timeout }
}

func (h *HeaderReader) ReadRequest() (req Request, err error) {
	var head, body []byte
	if head, err = h.r.Read(12); err != nil {
		return
	}

	body_len := bin_le.Uint32(head[4:8])
	if body, err = h.r.Read(int(body_len)); err != nil {
		return
	}

	req = Request{
		Msg:  iproto.RequestType(bin_le.Uint32(head[:4])),
		Body: body,
		Id:   bin_le.Uint32(head[8:12]),
	}

	return
}

func (h *HeaderReader) ReadResponse(retCodeLen int) (res Response, err error) {
	var code iproto.RetCode
	var head, body []byte

	if head, err = h.r.Read(12); err != nil {
		return
	}

	msg := iproto.RequestType(bin_le.Uint32(head[:4]))
	body_len := bin_le.Uint32(head[4:8])

	if msg != iproto.Ping {
		if body_len < uint32(retCodeLen) {
			code = iproto.RcProtocolError
		} else {
			body_len -= uint32(retCodeLen)
			switch retCodeLen {
			case 0:
				code = iproto.RcOK
			case 1:
				var c byte
				if c, err = h.r.ReadByte(); err != nil {
					return
				}
				code = iproto.RetCode(c)
			case 4:
				var cd []byte
				if cd, err = h.r.Read(4); err != nil {
					return
				}
				code = iproto.RetCode(bin_le.Uint32(cd))
			}
		}
	}

	if body, err = h.r.Read(int(body_len)); err != nil {
		return
	}

	res = Response{
		Id:   bin_le.Uint32(head[8:12]),
		Msg:  msg,
		Body: body,
		Code: code,
	}

	return
}

type HeaderWriter struct {
	w  BufWriter
}

func (h *HeaderWriter) Init(w io.Writer, timeout time.Duration) {
	h.w = BufWriter{ w: w, buf: make([]byte, 64*1024), timeout: timeout}
}

func (h *HeaderWriter) WriteRequest(req Request) (err error) {
	if err = h.w.Write3Uint32(uint32(req.Msg), uint32(len(req.Body)), uint32(req.Id)); err == nil {
		err = h.w.Write(req.Body)
	}
	return
}

func (h *HeaderWriter) WriteResponse(res Response, retCodeLen int) (err error) {
	if res.Msg == iproto.Ping {
		retCodeLen = 0
	}

	body_len := uint32(len(res.Body) + retCodeLen)
	if err = h.w.Write3Uint32(uint32(res.Msg), body_len, uint32(res.Id)); err != nil {
		return
	}

	switch retCodeLen {
	case 0:
	case 1:
		if err = h.w.WriteByte(byte(res.Code)); err != nil {
			return
		}
	case 4:
		if err = h.w.WriteUint32(uint32(res.Code)); err != nil {
			return
		}
	default:
		panic("Unsupported retCodeLen")
	}

	err = h.w.Write(res.Body)
	return
}

func (h *HeaderWriter) Flush() error {
	return h.w.Flush()
}
