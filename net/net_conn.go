package net

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

type RwcWrapper struct {
	io.ReadWriteCloser
}

func (r RwcWrapper) CloseWrite() error {
	return r.Close()
}

func (r RwcWrapper) CloseRead() error {
	return r.Close()
}

type RwcWrapperAddress bool

func (r RwcWrapperAddress) Network() string {
	return "virtual"
}

func (r RwcWrapperAddress) Address() string {
	return "wrapper"
}

func (r RwcWrapper) LocalAddr() (addr net.Addr) {
	return
}

func (r RwcWrapper) RemoteAddr() (addr net.Addr) {
	return
}
