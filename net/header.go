package net

import (
	"github.com/funny-falcon/go-iproto"
	"io"
	"encoding/binary"
	"log"
)

var bin_le = binary.LittleEndian

type Request struct {
	Msg      iproto.RequestType
	Body     []byte
	Id       uint32
}

type Response iproto.Response

type HeaderIO struct {
	buf                     [16]byte
	bodybuf                 []byte
}

const (
	bodybufSize = 1*1024
)

func (h *HeaderIO) Init() {
	h.bodybuf = make([]byte, bodybufSize)
}

func (h *HeaderIO) ReadRequest(r io.Reader) (req Request, err error) {
	if _, err := io.ReadFull(r, h.buf[:12]); err != nil {
		return Request{}, err
	}

	body_len := bin_le.Uint32(h.buf[4:8])
	req = Request{
		Msg:  iproto.RequestType(bin_le.Uint32(h.buf[:4])),
		Body: make([]byte, body_len),
		Id:   bin_le.Uint32(h.buf[8:12]),
	}

	_, err = io.ReadFull(r, req.Body)

	return
}

type readbyter interface {
	ReadByte() (c byte, err error)
}

func (h *HeaderIO) ReadResponse(r io.Reader, retCodeLen int) (res Response, err error) {
	var code iproto.RetCode

	if _, err := io.ReadFull(r, h.buf[:12]); err != nil {
		return Response{}, err
	}

	msg := iproto.RequestType(bin_le.Uint32(h.buf[:4]))
	body_len := bin_le.Uint32(h.buf[4:8])

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
				var err error
				switch rd := r.(type) {
				case readbyter:
					c, err = rd.ReadByte()
				default:
					_, err = io.ReadFull(r, h.buf[12:13])
					c = h.buf[12]
				}
				if err != nil {
					return Response{}, err
				}
				code = iproto.RetCode(c)
			case 4:
				if _, err = io.ReadFull(r, h.buf[12:16]); err != nil {
					return Response{}, err
				}
				code = iproto.RetCode(bin_le.Uint32(h.buf[12:16]))
			}
		}
	}

	var body []byte
	if body_len < 128 {
		if body_len > uint32(len(h.bodybuf)) {
			h.bodybuf = make([]byte, bodybufSize)
		}
		body = h.bodybuf[:body_len]
		h.bodybuf = h.bodybuf[body_len:]
	} else {
		body = make([]byte, body_len)
	}

	res = Response{
		Msg:  msg,
		Body: body,
		Code: code,
		Id:   bin_le.Uint32(h.buf[8:12]),
	}

	if len(res.Body) > 0 {
		_, err = io.ReadFull(r, res.Body)
	}

	return
}

func (h *HeaderIO) WriteRequest(w io.Writer, req Request) (err error) {
	bin_le.PutUint32(h.buf[:4], uint32(req.Msg))
	bin_le.PutUint32(h.buf[4:8], uint32(len(req.Body)))
	bin_le.PutUint32(h.buf[8:12], req.Id)

	if uint32(len(req.Body)) > 4 {
		log.Panicf("What are %+v", req)
	}

	if _, err = w.Write(h.buf[:12]); err == nil {
		_, err = w.Write(req.Body)
	}
	return
}

func (h *HeaderIO) WriteResponse(w io.Writer, res *Response, retCodeLen int) (err error) {
	var head []byte
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

	if _, err = w.Write(head); err == nil {
		_, err = w.Write(res.Body)
	}
	return
}
