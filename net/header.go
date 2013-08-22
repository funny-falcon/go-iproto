package net

import (
	"github.com/funny-falcon/go-iproto"
	"io"
	"bufio"
	"encoding/binary"
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

func (h *HeaderReader) Init(conn io.Reader) {
	h.r = SliceReader{ reader: conn, size: 16*1024 }
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
	buf [16]byte
	w  *bufio.Writer
}

func (h *HeaderWriter) Init(r io.Writer) {
	h.w = bufio.NewWriterSize(r, 64*1024)
}

func (h *HeaderWriter) WriteRequest(req Request) (err error) {
	bin_le.PutUint32(h.buf[:4], uint32(req.Msg))
	bin_le.PutUint32(h.buf[4:8], uint32(len(req.Body)))
	bin_le.PutUint32(h.buf[8:12], req.Id)

	if _, err = h.w.Write(h.buf[:12]); err == nil {
		_, err = h.w.Write(req.Body)
	}
	return
}

func (h *HeaderWriter) WriteResponse(res Response, retCodeLen int) (err error) {
	var head []byte

	if res.Msg == iproto.Ping {
		retCodeLen = 0
	}

	body_len := uint32(len(res.Body) + retCodeLen)
	bin_le.PutUint32(h.buf[:4], uint32(res.Msg))
	bin_le.PutUint32(h.buf[4:8], body_len)
	bin_le.PutUint32(h.buf[8:12], res.Id)

	switch retCodeLen {
	case 0:
		head = h.buf[:12]
	case 1:
		h.buf[12] = byte(res.Code)
		head = h.buf[:13]
	case 4:
		bin_le.PutUint32(h.buf[12:16], uint32(res.Code))
		head = h.buf[:16]
	default:
		panic("Unsupported retCodeLen")
	}

	if _, err = h.w.Write(head); err == nil {
		_, err = h.w.Write(res.Body)
	}
	return
}

func (h *HeaderWriter) Flush() error {
	return h.w.Flush()
}
