package broker

import (
	"fmt"
	"log/slog"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/server"
)

type Broker struct {
	config  BrokerConfig
	topics  *TopicManager
	handler *Handler
	server  *server.Server
	logger  *slog.Logger
}

// call Start() to serve
func NewBroker(config BrokerConfig, logger *slog.Logger) (*Broker, error) {
	applyBrokerDefaults(&config)

	tm, err := NewTopicManager(config.LogDir, config.BrokerID, config.LogConfig)
	if err != nil {
		return nil, fmt.Errorf("topic manager: %w", err)
	}

	handler := NewHandler(tm, config.BrokerID, config.Host, config.Port, logger)

	addr := fmt.Sprintf(":%d", config.Port)
	srv, err := server.NewServer(addr, handler, config.MaxRequestBytes, logger)
	if err != nil {
		tm.Close()
		return nil, fmt.Errorf("server: %w", err)
	}

	b := &Broker{
		config:  config,
		topics:  tm,
		handler: handler,
		server:  srv,
		logger:  logger,
	}

	logger.Info("broker initialised",
		"id", config.BrokerID,
		"addr", srv.Addr(),
		"logDir", config.LogDir)

	return b, nil
}

func (b *Broker) Start() {
	go b.server.Serve()
}

func (b *Broker) Shutdown() error {
	b.logger.Info("broker shutting down")
	b.server.Shutdown()

	if err := b.topics.Close(); err != nil {
		return fmt.Errorf("close topics: %w", err)
	}

	b.logger.Info("broker stopped")
	return nil
}

func (b *Broker) TopicManager() *TopicManager { return b.topics }
func (b *Broker) Addr() string                { return b.server.Addr().String() }
