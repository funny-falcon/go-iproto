package net_client

import (
	"log"
	"strings"
	"time"
)

type ServerConf struct {
	Network string
	Address string

	Name string

	Connections int

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PingInterval time.Duration

	RetCodeLen int
}

var DefaultReadTimeout = 30 * time.Second
var DefaultWriteTimeout = 30 * time.Second
var DefaultPingInterval = 1 * time.Second

func (cfg *ServerConf) SetDefaults() *ServerConf {
	if cfg.Address == "" {
		log.Panic("Could not init iproto.ClientServer with empty address")
	}

	if cfg.Network == "" {
		/* try to predict kind of network: if we have port separator, than it is tcp :) */
		if strings.ContainsRune(cfg.Address, ':') {
			cfg.Network = "tcp"
		} else {
			cfg.Network = "unix"
		}
	}

	if cfg.Name == "" {
		cfg.Name = cfg.Network + ":" + cfg.Address
	}

	if cfg.Connections <= 0 {
		cfg.Connections = 1
	}

	if cfg.PingInterval == 0 {
		cfg.PingInterval = DefaultPingInterval
	}

	return cfg
}
