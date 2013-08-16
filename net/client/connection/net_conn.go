package connection

import (
	"io"
	"net"
)

type NetConn interface {
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close() error
	CloseRead() error
	CloseWrite() error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

type rwcWrapper struct {
	io.ReadWriteCloser
}

func (r rwcWrapper) CloseWrite() error {
	return r.Close()
}

func (r rwcWrapper) CloseRead() error {
	return r.Close()
}

type rwcWrapperAddress bool

func (r rwcWrapperAddress) Network() string {
	return "virtual"
}

func (r rwcWrapperAddress) Address() string {
	return "wrapper"
}

func (r rwcWrapper) LocalAddr() (addr net.Addr) {
	return
}

func (r rwcWrapper) RemoteAddr() (addr net.Addr) {
	return
}
