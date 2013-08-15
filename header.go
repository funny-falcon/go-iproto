package iproto

import "io"
import "encoding/binary"
import "log"

var bin_le = binary.LittleEndian

type Header struct {
	head0, head1, head4     []byte
	rtype, rlen, rid, rcode []byte
	buf                     [16]byte
}

func (h *Header) Init() {
	h.head0, h.head1, h.head4 = h.buf[:12], h.buf[:13], h.buf[:16]
	h.rtype, h.rlen, h.rid = h.buf[:4], h.buf[4:8], h.buf[8:12]
	h.rcode = h.buf[12:16]
}

func (h *Header) ReadRequest(r io.Reader) (req RequestHeader, err error) {
	if _, err := io.ReadFull(r, h.head0); err != nil {
		return RequestHeader{}, err
	}

	body_len := bin_le.Uint32(h.rlen)
	req = RequestHeader{
		Msg:  RequestType(bin_le.Uint32(h.rtype)),
		Body: make([]byte, body_len),
		Id:   bin_le.Uint32(h.rid),
	}

	_, err = io.ReadFull(r, req.Body)

	return
}

type readbyter interface {
	ReadByte() (c byte, err error)
}

func (h *Header) ReadResponse(r io.Reader, retCodeLen int) (res Response, err error) {
	var code RetCode

	if _, err := io.ReadFull(r, h.head0); err != nil {
		return Response{}, err
	}

	msg := RequestType(bin_le.Uint32(h.rtype))
	body_len := bin_le.Uint32(h.rlen)

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
					_, err = io.ReadFull(r, h.rcode[:1])
					c = h.rcode[0]
				}
				if err != nil {
					return Response{}, err
				}
				code = RetCode(c)
			case 4:
				if _, err = io.ReadFull(r, h.rcode); err != nil {
					return Response{}, err
				}
				code = RetCode(bin_le.Uint32(h.rcode))
			}
		}
	}

	res = Response{
		Msg:  msg,
		Body: make([]byte, body_len),
		Code: code,
		Id:   bin_le.Uint32(h.rid),
	}

	if len(res.Body) > 0 {
		_, err = io.ReadFull(r, res.Body)
	}

	return
}

func (h *Header) WriteRequest(w io.Writer, req RequestHeader) (err error) {
	bin_le.PutUint32(h.rtype, uint32(req.Msg))
	bin_le.PutUint32(h.rlen, uint32(len(req.Body)))
	bin_le.PutUint32(h.rid, req.Id)

	if uint32(len(req.Body)) > 4 {
		log.Panicf("What are %+v", req)
	}

	if _, err = w.Write(h.head0); err == nil {
		_, err = w.Write(req.Body)
	}
	return
}

func (h *Header) WriteResponse(w io.Writer, res *Response, retCodeLen int) (err error) {
	var head []byte
	body_len := uint32(len(res.Body) + retCodeLen)
	bin_le.PutUint32(h.rtype, uint32(res.Msg))
	bin_le.PutUint32(h.rlen, body_len)
	bin_le.PutUint32(h.rid, res.Id)

	switch retCodeLen {
	case 0:
		head = h.head0
	case 1:
		h.rcode[0] = byte(res.Code)
		head = h.head1
	case 4:
		bin_le.PutUint32(h.rcode, uint32(res.Code))
		head = h.head4
	default:
		panic("Unsupported retCodeLen")
	}

	if _, err = w.Write(head); err == nil {
		_, err = w.Write(res.Body)
	}
	return
}
