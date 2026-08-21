package broker

import (
	"github.com/berkaydemircin/Distributed-Messaging-System/internal/log"
)

type BrokerConfig struct {
	BrokerID int32
	Host     string
	Port     int32

	LogDir          string
	LogConfig       log.LogConfig
	MaxRequestBytes int32

	Controller Controller
}

func applyBrokerDefaults(c *BrokerConfig) {
	if c.Host == "" {
		c.Host = "localhost"
	}
	if c.Port == 0 {
		c.Port = 9092
	}
	if c.LogDir == "" {
		c.LogDir = "/tmp/msgbroker-data"
	}
	if c.MaxRequestBytes <= 0 {
		c.MaxRequestBytes = 100 * 1024 * 1024 // 100 MB but this may be too much for default, maybe 10mb?
	}
}
