package controller

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const (
	defaultTransportMaxPool = 3
	defaultTransportTimeout = 10 * time.Second
	defaultSnapshotRetain   = 2
)

type ClusterNodeConfig struct {
	LocalID          raft.ServerID
	BindAddr         string
	AdvertiseAddr    string
	DataDir          string
	RaftConfig       *raft.Config
	TransportMaxPool int
	TransportTimeout time.Duration
	LogOutput        io.Writer
}

/*
	 Opens a persistent Raft node.
		bootstrapServers is used only when the node has no existing state.
*/
func NewClusterNode(
	cfg ClusterNodeConfig,
	bootstrapServers []raft.Server,
) (*Node, error) {
	if cfg.LocalID == "" {
		return nil, fmt.Errorf("controller cluster node: local ID is required")
	}
	if cfg.BindAddr == "" {
		return nil, fmt.Errorf("controller cluster node: bind address is required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("controller cluster node: data directory is required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("controller cluster node: create data directory: %w", err)
	}

	store, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("controller cluster node: open store: %w", err)
	}

	logOutput := cfg.LogOutput
	if logOutput == nil {
		logOutput = os.Stderr
	}
	snapshots, err := raft.NewFileSnapshotStore(
		cfg.DataDir,
		defaultSnapshotRetain,
		logOutput,
	)
	if err != nil {
		return nil, closeOnError(
			[]io.Closer{store},
			fmt.Errorf("controller cluster node: open snapshot store: %w", err),
		)
	}

	var advertise net.Addr
	if cfg.AdvertiseAddr != "" {
		advertise, err = net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr)
		if err != nil {
			return nil, closeOnError(
				[]io.Closer{store},
				fmt.Errorf("controller cluster node: resolve advertise address: %w", err),
			)
		}
	}

	maxPool := cfg.TransportMaxPool
	if maxPool == 0 {
		maxPool = defaultTransportMaxPool
	}
	if maxPool < 0 {
		return nil, closeOnError(
			[]io.Closer{store},
			fmt.Errorf("controller cluster node: transport max pool must be non-negative"),
		)
	}
	transportTimeout := cfg.TransportTimeout
	if transportTimeout == 0 {
		transportTimeout = defaultTransportTimeout
	}
	if transportTimeout < 0 {
		return nil, closeOnError(
			[]io.Closer{store},
			fmt.Errorf("controller cluster node: transport timeout must be non-negative"),
		)
	}

	transport, err := raft.NewTCPTransport(
		cfg.BindAddr,
		advertise,
		maxPool,
		transportTimeout,
		logOutput,
	)
	if err != nil {
		return nil, closeOnError(
			[]io.Closer{store},
			fmt.Errorf("controller cluster node: open transport: %w", err),
		)
	}

	return newNode(
		cfg.LocalID,
		cfg.RaftConfig,
		store,
		store,
		snapshots,
		transport,
		bootstrapServers,
		[]io.Closer{transport, store},
	)
}

func validateLocalBootstrapServer(
	localID raft.ServerID,
	localAddress raft.ServerAddress,
	servers []raft.Server,
) error {
	if len(servers) == 0 {
		return nil
	}
	for _, server := range servers {
		if server.ID != localID {
			continue
		}
		if server.Suffrage != raft.Voter {
			return fmt.Errorf("controller cluster node: local bootstrap server must be a voter")
		}
		if server.Address != localAddress {
			return fmt.Errorf(
				"controller cluster node: local bootstrap address %q does not match transport address %q",
				server.Address,
				localAddress,
			)
		}
		return nil
	}
	return fmt.Errorf("controller cluster node: bootstrap configuration does not contain local server %q", localID)
}
