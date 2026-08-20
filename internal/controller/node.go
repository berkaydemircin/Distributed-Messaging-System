package controller

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type NodeConfig struct {
	LocalID    raft.ServerID
	Address    raft.ServerAddress
	RaftConfig *raft.Config
}

type Node struct {
	raft          *raft.Raft
	fsm           *ControllerFSM
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
	transport     raft.Transport
	address       raft.ServerAddress
	closers       []io.Closer

	shutdownOnce sync.Once
	shutdownErr  error
}

func NewInMemNode(cfg NodeConfig) (*Node, error) {
	return newInMemNodeWithStores(
		cfg,
		raft.NewInmemStore(),
		raft.NewInmemSnapshotStore(),
	)
}

func newInMemNodeWithStores(
	cfg NodeConfig,
	logs *raft.InmemStore,
	snapshots *raft.InmemSnapshotStore,
) (*Node, error) {
	if cfg.LocalID == "" {
		return nil, fmt.Errorf("controller node: local ID is required")
	}
	if logs == nil {
		return nil, fmt.Errorf("controller node: log store is required")
	}
	if snapshots == nil {
		return nil, fmt.Errorf("controller node: snapshot store is required")
	}

	address, transport := raft.NewInmemTransport(cfg.Address)
	initial := []raft.Server{
		{ID: cfg.LocalID, Address: address, Suffrage: raft.Voter},
	}

	return newNode(
		cfg.LocalID,
		cfg.RaftConfig,
		logs,
		logs,
		snapshots,
		transport,
		initial,
		[]io.Closer{transport},
	)
}

func newNode(
	localID raft.ServerID,
	raftConfig *raft.Config,
	logStore raft.LogStore,
	stableStore raft.StableStore,
	snapshotStore raft.SnapshotStore,
	transport raft.Transport,
	bootstrapServers []raft.Server,
	closers []io.Closer,
) (*Node, error) {
	if localID == "" {
		return nil, closeOnError(closers, fmt.Errorf("controller node: local ID is required"))
	}
	if logStore == nil || stableStore == nil || snapshotStore == nil || transport == nil {
		return nil, closeOnError(closers, fmt.Errorf("controller node: Raft dependencies are required"))
	}

	config := makeRaftConfig(localID, raftConfig)
	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return nil, closeOnError(closers, fmt.Errorf("controller node: check existing state: %w", err))
	}
	if !hasState && len(bootstrapServers) > 0 {
		if err := validateLocalBootstrapServer(
			localID,
			transport.LocalAddr(),
			bootstrapServers,
		); err != nil {
			return nil, closeOnError(closers, err)
		}
		initial := raft.Configuration{Servers: bootstrapServers}
		if err := raft.BootstrapCluster(
			config,
			logStore,
			stableStore,
			snapshotStore,
			transport,
			initial,
		); err != nil {
			return nil, closeOnError(closers, fmt.Errorf("controller node: bootstrap: %w", err))
		}
	}

	fsm := NewControllerFSM()
	r, err := raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return nil, closeOnError(closers, fmt.Errorf("controller node: new raft: %w", err))
	}

	return &Node{
		raft:          r,
		fsm:           fsm,
		logStore:      logStore,
		stableStore:   stableStore,
		snapshotStore: snapshotStore,
		transport:     transport,
		address:       transport.LocalAddr(),
		closers:       closers,
	}, nil
}

func makeRaftConfig(localID raft.ServerID, configured *raft.Config) *raft.Config {
	var config raft.Config
	if configured == nil {
		config = *raft.DefaultConfig()
	} else {
		config = *configured
	}
	config.LocalID = localID
	return &config
}

func closeOnError(closers []io.Closer, cause error) error {
	errs := []error{cause}
	for _, closer := range closers {
		if closer != nil {
			errs = append(errs, closer.Close())
		}
	}
	return errors.Join(errs...)
}

func (n *Node) FSM() *ControllerFSM {
	return n.fsm
}

func (n *Node) State() raft.RaftState {
	return n.raft.State()
}

func (n *Node) Address() raft.ServerAddress {
	return n.address
}

// returns the currently observed leader
func (n *Node) LeaderWithID() (raft.ServerAddress, raft.ServerID) {
	return n.raft.LeaderWithID()
}

// waits until local node becomes the leader
func (n *Node) WaitForLeader(timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("controller node: leader timeout must be positive")
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		if n.raft.State() == raft.Leader {
			return nil
		}
		if n.raft.State() == raft.Shutdown {
			return raft.ErrRaftShutdown
		}

		select {
		case <-timer.C:
			return fmt.Errorf("controller node: no leader within %s", timeout)
		case <-ticker.C:
		}
	}
}

func (n *Node) Apply(cmdType CommandType, payload interface{}, timeout time.Duration) error {
	data, err := EncodeCommand(cmdType, payload)
	if err != nil {
		return err
	}

	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("controller node: raft apply: %w", err)
	}

	switch response := future.Response().(type) {
	case nil:
		return nil
	case error:
		return response
	default:
		return fmt.Errorf("controller node: unexpected FSM response %T", response)
	}
}

func (n *Node) AddVoter(
	id raft.ServerID,
	address raft.ServerAddress,
	previousIndex uint64,
	timeout time.Duration,
) error {
	if id == "" || address == "" {
		return fmt.Errorf("controller node: voter ID and address are required")
	}
	if err := n.raft.AddVoter(id, address, previousIndex, timeout).Error(); err != nil {
		return fmt.Errorf("controller node: add voter %q: %w", id, err)
	}
	return nil
}

func (n *Node) RemoveServer(
	id raft.ServerID,
	previousIndex uint64,
	timeout time.Duration,
) error {
	if id == "" {
		return fmt.Errorf("controller node: server ID is required")
	}
	if err := n.raft.RemoveServer(id, previousIndex, timeout).Error(); err != nil {
		return fmt.Errorf("controller node: remove server %q: %w", id, err)
	}
	return nil
}

// Returns the current Raft membership.
func (n *Node) Configuration() (raft.Configuration, error) {
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return raft.Configuration{}, fmt.Errorf("controller node: get configuration: %w", err)
	}
	return future.Configuration(), nil
}

func (n *Node) Shutdown() error {
	n.shutdownOnce.Do(func() {
		errs := []error{n.raft.Shutdown().Error()}
		for _, closer := range n.closers {
			if closer != nil {
				errs = append(errs, closer.Close())
			}
		}
		n.shutdownErr = errors.Join(errs...)
	})
	return n.shutdownErr
}
