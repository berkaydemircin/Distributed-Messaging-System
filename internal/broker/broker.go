package broker

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"strconv"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/controller"
	"github.com/berkaydemircin/Distributed-Messaging-System/internal/server"
)

type Broker struct {
	config        BrokerConfig
	topics        *TopicManager
	handler       *Handler
	server        *server.Server
	logger        *slog.Logger
	incarnationID string
	fetchManager  *FetchManager
}

// broker controller metadata operations, could be its own seperate controller.go in this dir
// but for now this is where its saying
type Controller interface {
	RegisterBroker(
		id int32,
		incarnationID string,
		endpoints []controller.BrokerEndpoint,
	) error
	UnfenceBroker(id int32, brokerEpoch uint64) error

	Broker(id int32) (controller.BrokerInfo, bool)
	PartitionsForBroker(id int32) []controller.PartitionMetadata
	AllPartitions() []controller.PartitionMetadata
	AllBrokers() []controller.BrokerInfo
	Partition(
		topic string,
		partition int32,
	) (controller.PartitionMetadata, bool)
}

var _ Controller = (*controller.LeaderAdapter)(nil)

// call Start() to serve
func NewBroker(config BrokerConfig, logger *slog.Logger) (*Broker, error) {
	applyBrokerDefaults(&config)

	tm, err := NewTopicManager(config.LogDir, config.BrokerID, config.LogConfig)
	if err != nil {
		return nil, fmt.Errorf("topic manager: %w", err)
	}

	handler := NewHandler(tm, config.BrokerID, config.Host, config.Port, logger, config.Controller)

	addr := fmt.Sprintf(":%d", config.Port)
	srv, err := server.NewServer(addr, handler, config.MaxRequestBytes, logger)
	if err != nil {
		tm.Close()
		return nil, fmt.Errorf("server: %w", err)
	}

	incarnationID, err := generateIncarnationID()
	if err != nil {
		srv.Shutdown()
		tm.Close()
		return nil, fmt.Errorf("generate incarnation id: %w", err)
	}

	b := &Broker{
		config:        config,
		topics:        tm,
		handler:       handler,
		server:        srv,
		logger:        logger,
		incarnationID: incarnationID,
	}

	if _, port, err := listenerEndpoint(srv.Addr().String(), config.Host); err == nil {
		handler.port = int32(port)
	}

	logger.Info("broker initialised",
		"id", config.BrokerID,
		"addr", srv.Addr(),
		"logDir", config.LogDir)

	return b, nil
}

// Start registers with the controller before serving requests.
func (b *Broker) Start() error {
	if b.config.Controller != nil {
		if err := b.registerAndReconcile(); err != nil {
			return err
		}
	} else {
		b.logger.Info("no controller configured, running standalone")
	}

	go b.server.Serve()
	return nil
}

func (b *Broker) registerAndReconcile() error {
	ctrl := b.config.Controller

	host, port, err := listenerEndpoint(b.server.Addr().String(), b.config.Host)
	if err != nil {
		return err
	}

	endpoints := []controller.BrokerEndpoint{
		{Name: "PLAINTEXT", Host: host, Port: port, SecurityProtocol: controller.SecurityProtocolPlaintext},
	}

	if err := ctrl.RegisterBroker(b.config.BrokerID, b.incarnationID, endpoints); err != nil {
		return fmt.Errorf("register broker: %w", err)
	}

	info, ok := ctrl.Broker(b.config.BrokerID)
	if !ok {
		return fmt.Errorf("broker %d not found immediately after registration", b.config.BrokerID)
	}

	if err := b.topics.Reconcile(ctrl, b.config.BrokerID); err != nil {
		return fmt.Errorf("reconcile partition assignments: %w", err)
	}

	b.fetchManager = NewFetchManager(b.config.BrokerID, b.topics, ctrl, b.logger)
	if err := b.fetchManager.Start(); err != nil {
		b.fetchManager = nil
		return fmt.Errorf("start fetch manager: %w", err)
	}

	if err := ctrl.UnfenceBroker(b.config.BrokerID, info.BrokerEpoch); err != nil {
		b.fetchManager.Stop()
		b.fetchManager = nil
		return fmt.Errorf("unfence broker: %w", err)
	}

	b.logger.Info("registered and reconciled with controller",
		"id", b.config.BrokerID, "incarnationId", b.incarnationID, "brokerEpoch", info.BrokerEpoch)
	return nil
}

func listenerEndpoint(addr, advertisedHost string) (string, uint16, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("parse listener address: %w", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return "", 0, fmt.Errorf("parse listener port: %w", err)
	}
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = advertisedHost
	}
	return host, uint16(port), nil
}

func generateIncarnationID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func (b *Broker) Shutdown() error {
	b.logger.Info("broker shutting down")
	if b.fetchManager != nil {
		b.fetchManager.Stop()
	}
	b.server.Shutdown()

	if err := b.topics.Close(); err != nil {
		return fmt.Errorf("close topics: %w", err)
	}

	b.logger.Info("broker stopped")
	return nil
}

func (b *Broker) TopicManager() *TopicManager { return b.topics }
func (b *Broker) Addr() string                { return b.server.Addr().String() }
