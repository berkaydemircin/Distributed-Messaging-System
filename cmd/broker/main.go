package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/broker"
	"github.com/berkaydemircin/Distributed-Messaging-System/internal/log"
)

func main() {
	var (
		brokerID        = flag.Int("broker-id", 0, "unique broker ID")
		host            = flag.String("host", "localhost", "advertised hostname")
		port            = flag.Int("port", 9092, "listener port")
		logDir          = flag.String("log-dir", "/tmp/msgbroker-data", "data directory")
		segmentBytes    = flag.Int64("segment-bytes", 1<<30, "max segment size (bytes)")
		indexBytes      = flag.Int64("index-bytes", 1<<20, "max index size (bytes)")
		indexInterval   = flag.Int64("index-interval", 4096, "index interval (bytes)")
		logLevel        = flag.String("log-level", "info", "log level: debug, info, warn, error")
	)
	flag.Parse()

	// structured logger
	var level slog.Level
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	cfg := broker.BrokerConfig{
		BrokerID: int32(*brokerID),
		Host:     *host,
		Port:     int32(*port),
		LogDir:   *logDir,
		LogConfig: log.LogConfig{
			MaxSegmentBytes:    *segmentBytes,
			MaxIndexBytes:      *indexBytes,
			IndexIntervalBytes: *indexInterval,
		},
	}

	b, err := broker.NewBroker(cfg, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}

	b.Start()

	logger.Info("broker ready", "addr", b.Addr())

	// wait for SIGINT / SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("received signal", "signal", sig)

	if err := b.Shutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown error: %v\n", err)
		os.Exit(1)
	}
}
