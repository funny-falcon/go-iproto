package server

import (
	"github.com/funny-falcon/go-iproto"
	"time"
)

type Config struct {
	Network string
	Address string

	EndPoint iproto.Service

	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	RetCodeLen int

}
