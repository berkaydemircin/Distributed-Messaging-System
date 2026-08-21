package controller

import (
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

var ErrNotControllerLeader = errors.New("controller: local node is not the current leader")

const defaultApplyTimeout = 5 * time.Second

type LeaderAdapter struct {
	node *Node
}

func NewLeaderAdapter(node *Node) *LeaderAdapter {
	return &LeaderAdapter{node: node}
}

func (a *LeaderAdapter) RegisterBroker(id int32, incarnationID string, endpoints []BrokerEndpoint) error {
	err := a.node.Apply(CommandRegisterBroker, RegisterBrokerCommand{
		ID: id, IncarnationID: incarnationID, Endpoints: endpoints,
	}, defaultApplyTimeout)
	return translateLeaderError(err)
}

func (a *LeaderAdapter) UnfenceBroker(id int32, brokerEpoch uint64) error {
	err := a.node.Apply(CommandUnfenceBroker, UnfenceBrokerCommand{
		BrokerID: id, BrokerEpoch: brokerEpoch,
	}, defaultApplyTimeout)
	return translateLeaderError(err)
}

func (a *LeaderAdapter) Broker(id int32) (BrokerInfo, bool) {
	return a.node.FSM().GetBroker(id)
}

func (a *LeaderAdapter) PartitionsForBroker(id int32) []PartitionMetadata {
	return a.node.FSM().PartitionsForBroker(id)
}

func (a *LeaderAdapter) AllPartitions() []PartitionMetadata {
	return a.node.FSM().AllPartitions()
}

func (a *LeaderAdapter) AllBrokers() []BrokerInfo {
	return a.node.FSM().AllBrokers()
}

func (a *LeaderAdapter) Partition(topic string, partition int32) (PartitionMetadata, bool) {
	return a.node.FSM().GetPartitionMetadata(topic, partition)
}

func translateLeaderError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, raft.ErrNotLeader) {
		return fmt.Errorf("%w: %v", ErrNotControllerLeader, err)
	}
	return err
}
