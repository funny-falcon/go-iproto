package server

import (
	"github.com/funny-falcon/go-iproto"
	"github.com/funny-falcon/go-iproto/net"
	"time"
)

type Config struct {
	Network string
	Address string

	EndPoint iproto.Service

	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	RetCodeType net.RCType
}
