package iproto

import "io"
import "encoding/binary"
import "log"

var bin_le = binary.LittleEndian

type Header struct {
	buf                     [16]byte
}

func (h *Header) Init() {
}

func (h *Header) ReadRequest(r io.Reader) (req RequestHeader, err error) {
	if _, err := io.ReadFull(r, h.buf[:12]); err != nil {
		return RequestHeader{}, err
	}

	body_len := bin_le.Uint32(h.buf[4:8])
	req = RequestHeader{
		Msg:  RequestType(bin_le.Uint32(h.buf[:4])),
		Body: make([]byte, body_len),
		Id:   bin_le.Uint32(h.buf[8:12]),
	}

	_, err = io.ReadFull(r, req.Body)

	return
}

type readbyter interface {
	ReadByte() (c byte, err error)
}

func (h *Header) ReadResponse(r io.Reader, retCodeLen int) (res Response, err error) {
	var code RetCode

	if _, err := io.ReadFull(r, h.buf[:12]); err != nil {
		return Response{}, err
	}

	msg := RequestType(bin_le.Uint32(h.buf[:4]))
	body_len := bin_le.Uint32(h.buf[4:8])

	if msg != Ping {
		if body_len < uint32(retCodeLen) {
			code = RcProtocolError
		} else {
			body_len -= uint32(retCodeLen)
			switch retCodeLen {
			case 0:
				code = RcOK
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
				code = RetCode(c)
			case 4:
				if _, err = io.ReadFull(r, h.buf[12:16]); err != nil {
					return Response{}, err
				}
				code = RetCode(bin_le.Uint32(h.buf[12:16]))
			}
		}
	}

	res = Response{
		Msg:  msg,
		Body: make([]byte, body_len),
		Code: code,
		Id:   bin_le.Uint32(h.buf[8:12]),
	}

	if len(res.Body) > 0 {
		_, err = io.ReadFull(r, res.Body)
	}

	return
}

func (h *Header) WriteRequest(w io.Writer, req RequestHeader) (err error) {
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

func (h *Header) WriteResponse(w io.Writer, res *Response, retCodeLen int) (err error) {
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
