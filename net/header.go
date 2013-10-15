package net

import (
	"encoding/binary"
	"errors"
	"github.com/funny-falcon/go-iproto"
	"io"
	"time"
)

var bin_le = binary.LittleEndian

type Request struct {
	Msg  iproto.RequestType
	Body []byte
	Id   uint32
}

type Response iproto.Response

type HeaderReader struct {
	r  SliceReader
	rc RCType
}

func (h *HeaderReader) Init(conn io.Reader, timeout time.Duration, rc RCType) {
	h.r = SliceReader{r: conn, size: 8 * 1024, timeout: timeout}
	h.rc = rc
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

func (h *HeaderReader) ReadResponse() (res Response, err error) {
	var code iproto.RetCode
	var head, body []byte

	if head, err = h.r.Read(12); err != nil {
		return
	}

	msg := iproto.RequestType(bin_le.Uint32(head[:4]))
	body_len := bin_le.Uint32(head[4:8])

	if msg != iproto.Ping {
		switch h.rc {
		case RC0byte:
			code = iproto.RcOK
		case RC1byte:
			if body_len < 1 {
				code = iproto.RcProtocolError
			} else {
				var c byte
				body_len -= 1
				if c, err = h.r.ReadByte(); err != nil {
					return
				}
				code = iproto.RetCode(c)
			}
		case RC4byte:
			if body_len < 4 {
				code = iproto.RcProtocolError
			} else {
				var cd []byte
				body_len -= 4
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

func (h *HeaderReader) ReadPing() (err error) {
	var head []byte
	if head, err = h.r.Read(12); err != nil {
		return
	}

	msg := iproto.RequestType(bin_le.Uint32(head[:4]))
	id := bin_le.Uint32(head[8:])

	if msg != iproto.Ping || id != iproto.PingRequestId {
		err = errors.New("Iproto ping failed")
	}
	return
}

type HeaderWriter struct {
	w   BufWriter
	rc  RCType
	rcl int
}

func (h *HeaderWriter) Init(w io.Writer, timeout time.Duration, rc RCType) {
	h.w = BufWriter{w: w, timeout: timeout}
	h.rc = rc
	switch h.rc {
	case RC0byte:
		h.rcl = 0
	case RC1byte:
		h.rcl = 1
	case RC4byte:
		h.rcl = 4
	default:
		panic("Unsupported return code len")
	}
}

func (h *HeaderWriter) WriteRequest(req Request) (err error) {
	if err = h.w.Write3Uint32(uint32(req.Msg), uint32(len(req.Body)), uint32(req.Id)); err == nil {
		err = h.w.Write(req.Body)
	}
	return
}

func (h *HeaderWriter) Ping() (err error) {
	ping := Request{
		Msg:  iproto.Ping,
		Body: make([]byte, 0),
		Id:   iproto.PingRequestId,
	}
	return h.WriteRequest(ping)
}

func (h *HeaderWriter) WriteResponse(res Response) (err error) {
	retCodeLen := h.rcl
	if res.Msg == iproto.Ping {
		retCodeLen = 0
	}
	body_len := uint32(len(res.Body) + retCodeLen)
	if err = h.w.Write3Uint32(uint32(res.Msg), body_len, uint32(res.Id)); err != nil {
		return
	}

	switch h.rc {
	case RC0byte:
	case RC1byte:
		if err = h.w.WriteByte(byte(res.Code)); err != nil {
			return
		}
	case RC4byte:
		if err = h.w.WriteUint32(uint32(res.Code)); err != nil {
			return
		}
	default:
		panic("Unsupported return code len")
	}

	err = h.w.Write(res.Body)
	return
}

func (h *HeaderWriter) Flush() error {
	return h.w.Flush()
}
