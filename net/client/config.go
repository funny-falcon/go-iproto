package client

import (
	"log"
	"strings"
	"time"
)

type ServerConfig struct {
	Network string
	Address string

	Name string

	Connections int

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PingInterval time.Duration

	RetCodeLen int

	Timeout time.Duration
}

var DefaultReadTimeout = 30 * time.Second
var DefaultWriteTimeout = 30 * time.Second
var DefaultPingInterval = 1 * time.Second

func (cfg *ServerConfig) SetDefaults() *ServerConfig {
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

	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}

	return cfg
}
